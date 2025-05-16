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

	// defer td.cancel()
	defer func () {
		td.cancel()
		close(td.alertChan)
	}()


	// go func() {
	// 	for {
	// 		select {
	// 		case alert := <-td.alertChan:
	// 			go func(msg *alertMsg) {
	// 				var e error
	// 				e = notifyPagerduty(msg)
	// 				if e != nil {
	// 					l(msg.chain, "error sending alert to pagerduty", e.Error())
	// 				}
	// 				e = notifyDiscord(msg)
	// 				if e != nil {
	// 					l(msg.chain, "error sending alert to discord", e.Error())
	// 				}
	// 				e = notifyTg(msg)
	// 				if e != nil {
	// 					l(msg.chain, "error sending alert to telegram", e.Error())
	// 				}
	// 				e = notifySlack(msg)
	// 				if e != nil {
	// 					l(msg.chain, "error sending alert to slack", e.Error())
	// 				}
	// 			}(alert)
	// 		case <-td.ctx.Done():
	// 			return
	// 		}
	// 	}
	// }()

	// 단일 워커로 동기 처리: goroutine 폭증 방지
	go func() {
	    for msg := range td.alertChan {
	        if e := notifyPagerduty(msg); e != nil {
	            l(msg.chain, "error sending alert to pagerduty", e.Error())
	        }
	        if e := notifyDiscord(msg); e != nil {
	            l(msg.chain, "error sending alert to discord", e.Error())
	        }
	        if e := notifyTg(msg); e != nil {
	            l(msg.chain, "error sending alert to telegram", e.Error())
	        }
	        if e := notifySlack(msg); e != nil {
	            l(msg.chain, "error sending alert to slack", e.Error())
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
        Nodes:   make([]*NodeConfig, len(td.Chains["B-Harvest"].Nodes)),
    }
    for i, n := range td.Chains["B-Harvest"].Nodes {
        pivot.Nodes[i] = &NodeConfig{
            Url:         n.Url,
            AlertIfDown: n.AlertIfDown,
        }
    }

    // RPC 클라이언트 초기화
    if err := pivot.newRpc(); err != nil {
        return err
    }

    // ─────────── 여기부터 수정: 전체 Validator 셋 페이지네이션으로 가져오기 ───────────

    pageReq := &query.PageRequest{Key: nil, Limit: 500}
    var allVals []staking.Validator

    for {
        // 1) 페이징 요청 생성
        req := &staking.QueryValidatorsRequest{
            Status:     staking.BondStatusBonded,
            Pagination: pageReq,
        }
        bz, err := req.Marshal()
        if err != nil {
            return err
        }

        // 2) ABCIQuery로 Validators 호출 (경로: "/cosmos.staking.v1beta1.Query/Validators")
        resp, err := pivot.client.ABCIQuery(td.ctx, "/cosmos.staking.v1beta1.Query/Validators", bz)
        if err != nil {
            return err
        }
        if resp.Response.Value == nil {
            return errors.New("could not find validators (empty response)")
        }

        // 3) 응답 언마샬링
        var pageRes staking.QueryValidatorsResponse
        if err := pageRes.Unmarshal(resp.Response.Value); err != nil {
            return err
        }

        // 4) 로그 출력 (페이징별 개수)
        log.Printf("[Validators] fetched %d validators in this page\n", len(pageRes.Validators))
        allVals = append(allVals, pageRes.Validators...)

        // 5) 더 가져올 페이지가 없으면 종료
        if pageRes.Pagination.NextKey == nil || len(pageRes.Pagination.NextKey) == 0 {
            break
        }
        pageReq.Key = pageRes.Pagination.NextKey
    }

    // 6) 전체 개수 로그
    log.Printf("[Validators] total bonded validators fetched: %d\n", len(allVals))

    // ─────────── 기존 로직: td.Chains 맵 초기화 ───────────

    cnt := 0
    for _, val := range allVals {
        nodes := []*NodeConfig{{
            Url:         pivot.Nodes[cnt%len(pivot.Nodes)].Url,
            AlertIfDown: pivot.Nodes[cnt%len(pivot.Nodes)].AlertIfDown,
        }}
        c := &ChainConfig{
            name:            val.GetMoniker(),
            blocksResults:   make([]int, showBLocks),
            ChainId:         pivot.ChainId,
            ValAddress:      val.OperatorAddress,
            ValconsOverride: pivot.ValconsOverride,
            ExtraInfo:       pivot.ExtraInfo,
            Alerts:          pivot.Alerts,
            PublicFallback:  pivot.PublicFallback,
            Nodes:           nodes,
        }
        for i := range c.blocksResults {
            c.blocksResults[i] = 3
        }
        td.Chains[val.GetMoniker()] = c
        cnt++
    }

    // ─────────── 나머지 로직 (모니터링 루프, 상태 저장 등) ───────────

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

            // websocket subscription and periodic validator info 갱신
            for {
                if err := cc.newRpc(); err != nil {
                    l(cc.ChainId, err)
                    time.Sleep(5 * time.Second)
                    continue
                }
                if err := cc.GetValInfo(true); err != nil {
                    l("🛑", cc.ChainId, err)
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
