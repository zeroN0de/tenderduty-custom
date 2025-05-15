package tenderduty

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	dash "github.com/blockpane/tenderduty/v2/td2/dashboard"
	"github.com/cosmos/cosmos-sdk/types/query"
	staking "github.com/cosmos/cosmos-sdk/x/staking/types"

)

var td = &Config{}

func Run(configFile, stateFile, chainConfigDirectory string, password *string) error {
	var err error
	td, err = loadConfig(configFile, stateFile, chainConfigDirectory, password)
	if err != nil {
		return err
	}
	fatal, problems := validateConfig(td)
	for _, p := range problems {
		fmt.Println(p)
	}
	if fatal {
		log.Fatal("tenderduty the configuration is invalid, refusing to start")
	}
	log.Println("tenderduty config is valid, starting tenderduty with", len(td.Chains), "chains")

	defer td.cancel()

	go func() {
		for {
			select {
			case alert := <-td.alertChan:
				go func(msg *alertMsg) {
					var e error
					e = notifyPagerduty(msg)
					if e != nil {
						l(msg.chain, "error sending alert to pagerduty", e.Error())
					}
					e = notifyDiscord(msg)
					if e != nil {
						l(msg.chain, "error sending alert to discord", e.Error())
					}
					e = notifyTg(msg)
					if e != nil {
						l(msg.chain, "error sending alert to telegram", e.Error())
					}
					e = notifySlack(msg)
					if e != nil {
						l(msg.chain, "error sending alert to slack", e.Error())
					}
				}(alert)
			case <-td.ctx.Done():
				return
			}
		}
	}()

	if td.EnableDash {
		go dash.Serve(td.Listen, td.updateChan, td.logChan, td.HideLogs)
		l("starting dashboard on", td.Listen)
	} else {
		go func() {
			for {
				<-td.updateChan
			}
		}()
	}
	if td.Prom {
		go prometheusExporter(td.ctx, td.statsChan)
	} else {
		go func() {
			for {
				<-td.statsChan
			}
		}()
	}

	pivot := ChainConfig{
		ChainId: td.Chains["B-Harvest"].ChainId,
		Nodes:  make([]*NodeConfig, len(td.Chains["B-Harvest"].Nodes)),
	}
	for i, n := range td.Chains["B-Harvest"].Nodes {
		nc := NodeConfig{
			Url: n.Url,
			AlertIfDown: n.AlertIfDown,
		}

		pivot.Nodes[i] = &nc
	}

	err = pivot.newRpc()
	if err != nil {
		return err
	}

	q := staking.QueryValidatorsRequest{
		Status: staking.BondStatusBonded,
		Pagination: &query.PageRequest{
			Limit: 500,
		},
	}
	b, err := q.Marshal()
	if err != nil {
		return err
	}
	resp, err := pivot.client.ABCIQuery(td.ctx, "/cosmos.staking.v1beta1.Query/Validators", b)
	if err != nil {
		return err
	}
	if resp.Response.Value == nil {
		return errors.New("could not find validators")
	}
	vals := &staking.QueryValidatorsResponse{}
	err = vals.Unmarshal(resp.Response.Value)
	if err != nil {
		return err
	}

	cnt := 0
	for _, val := range vals.Validators {
		nodes := make([]*NodeConfig, 1)
		nodes[0] = &NodeConfig{
			Url: pivot.Nodes[cnt%len(pivot.Nodes)].Url,
			AlertIfDown: pivot.Nodes[cnt%len(pivot.Nodes)].AlertIfDown,
		}
		c := &ChainConfig{
			name:            val.GetMoniker(),
			blocksResults:   make([]int, showBLocks),
			ChainId:         pivot.ChainId,
			ValAddress:      val.OperatorAddress,
			ValconsOverride: pivot.ValconsOverride,
			ExtraInfo:       pivot.ExtraInfo,
			Alerts:          pivot.Alerts,
			PublicFallback:  pivot.PublicFallback,
			// Nodes:           []*NodeConfig{pivot.Nodes[cnt%len(pivot.Nodes)]},
			Nodes: nodes,
		}
		for i := 0; i < showBLocks; i++ {
			c.blocksResults[i] = 3
		}
		td.Chains[val.GetMoniker()] = c

		cnt++
	}

	for k := range td.Chains {
		cc := td.Chains[k]

		go func(cc *ChainConfig, name string) {
			// alert worker
			go cc.watch()

			// node health checks:
			go func() {
				for {
					cc.monitorHealth(td.ctx, name)
				}
			}()

			// websocket subscription and occasional validator info refreshes
			for {
				e := cc.newRpc()
				if e != nil {
					l(cc.ChainId, e)
					time.Sleep(5 * time.Second)
					continue
				}
				e = cc.GetValInfo(true)
				if e != nil {
					l("🛑", cc.ChainId, e)
				}
				cc.WsRun()
				l(cc.ChainId, "🌀 websocket exited! Restarting monitoring")
				time.Sleep(5 * time.Second)
			}
		}(cc, k)
	}

	// attempt to save state on exit, only a best-effort ...
	saved := make(chan interface{})
	go saveOnExit(stateFile, saved)

	<-td.ctx.Done()
	<-saved

	return err
}

func saveOnExit(stateFile string, saved chan interface{}) {
	quitting := make(chan os.Signal, 1)
	signal.Notify(quitting, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	saveState := func() {
		defer close(saved)
		log.Println("saving state...")
		//#nosec -- variable specified on command line
		f, e := os.OpenFile(stateFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if e != nil {
			log.Println(e)
			return
		}
		td.chainsMux.Lock()
		defer td.chainsMux.Unlock()
		blocks := make(map[string][]int)
		// only need to save counts if the dashboard exists
		if td.EnableDash {
			for k, v := range td.Chains {
				blocks[k] = v.blocksResults
			}
		}
		nodesDown := make(map[string]map[string]time.Time)
		for k, v := range td.Chains {
			for _, node := range v.Nodes {
				if node.down {
					if nodesDown[k] == nil {
						nodesDown[k] = make(map[string]time.Time)
					}
					nodesDown[k][node.Url] = node.downSince
				}
			}
		}
		b, e := json.Marshal(&savedState{
			Alarms:    alarms,
			Blocks:    blocks,
			NodesDown: nodesDown,
		})
		if e != nil {
			log.Println(e)
			return
		}
		_, _ = f.Write(b)
		_ = f.Close()
		log.Println("tenderduty exiting.")
	}
	for {
		select {
		case <-td.ctx.Done():
			saveState()
			return
		case <-quitting:
			saveState()
			td.cancel()
			return
		}
	}
}
