package main

import (
	"fmt"
	"os"
	"time"

	"github.com/go-numb/go-bitflyer/v1/public/executions"

	influx "github.com/influxdata/influxdb1-client/v2"
	"github.com/labstack/gommon/log"

	"github.com/go-numb/go-bitflyer/v1/jsonrpc"
)

const (
	// $ influx
	// $ > create exec
	// $ > use exec
	DBNAME = "exec"

	LOGOUTPUTLEVEL = log.INFO
)

func init() {
	// setup Logger
	f, err := os.OpenFile("server.log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	log.SetLevel(LOGOUTPUTLEVEL)
	log.SetOutput(f)
}

func main() {
	done := make(chan struct{})

	// // 購読チャンネルを配列で設定
	// データベースを同数用意し各reading sectionに渡す
	channels := []string{"lightning_executions_FX_BTC_JPY"} // "lightning_executions_BTC_JPY"

	for _, v := range channels {
		go reading(v)
	}

	<-done
}

// reading section
func reading(channel string) {
Reconnect:
	ch := make(chan jsonrpc.Response)
	go jsonrpc.Get([]string{
		channel,
	}, ch)
	time.Sleep(time.Second)

	defer close(ch)

	/*
		# 時系列データベースセクション
	*/
	inf, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:     "http://localhost:8086",
		Username: "admin",
		Password: "admin",
	})
	if err != nil {
		log.Fatal("Error: ", err)
	}
	defer inf.Close()

	// Create a new point batch
	bp, err := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:  DBNAME,
		Precision: "s",
	})
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case v := <-ch:
			switch v.Type {
			case jsonrpc.Executions:
				go set(inf, bp, v.Executions)
				fmt.Printf("%+v\n", v.Executions)

			case jsonrpc.Error:
				log.Error("websocket reconnect error, ", v.Error.Error())
				goto wsError

			}
		}
	}

wsError:
	close(ch)
	time.Sleep(3 * time.Second)
	if time.Now().Hour() == 4 { // メンテナンス時間に落ちた場合
		log.Infof("now time %s, therefore waiting websocket reconnect 12minutes", time.Now().Format("15:04"))
		time.Sleep(15 * time.Minute)
		log.Info("end bitflyer mentenance time, reconnect websocket")
	}
	goto Reconnect
}

func set(inf influx.Client, bp influx.BatchPoints, exes executions.Response) error {
	for _, v := range exes {
		point, err := influx.NewPoint("bitflyer", map[string]string{"currency": "fxbtcjpy"}, map[string]interface{}{
			"execution": &v,
		}, time.Now().UTC())
		if err != nil {
			log.Error(err)
			return err
		}
		bp.AddPoint(point)
	}

	if err := inf.Write(bp); err != nil {
		log.Error(err)
		return err
	}

	return nil
}
