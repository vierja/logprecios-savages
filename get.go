package main

import (
	"encoding/json"
	"flag"
	"github.com/nutrun/lentil"
	"github.com/vierja/logprecios-parsers"
	"log"
	"net"
	"net/url"
	"os"
	"runtime"
	"strings"
	"syscall"
	"time"
)

type SharedBeanstalkd struct {
	Beanstalkd *lentil.Beanstalkd
	Sem        chan bool
}

type BeanstalkSettings struct {
	Addr         string
	WatchChannel string
	UseChannel   string
}

type Worker struct {
	WorkerId     int
	Beanstalkd   *lentil.Beanstalkd
	Stats        *GetStats
	ConnSettings BeanstalkSettings
}

func (this *Worker) Connect() error {

	var e error
	this.Beanstalkd, e = lentil.Dial(this.ConnSettings.Addr)
	if e != nil {
		return e
	}

	if this.ConnSettings.WatchChannel != "" {
		_, e = this.Beanstalkd.Watch(this.ConnSettings.WatchChannel)
		if e != nil {
			return e
		}
	}

	if this.ConnSettings.UseChannel != "" {
		e = this.Beanstalkd.Use(this.ConnSettings.UseChannel)
		if e != nil {
			return e
		}
	}

	return nil
}

func (this *Worker) processMessage() error {
	job, e := this.Beanstalkd.Reserve()
	if e != nil {
		log.Printf(e.Error())
		return e
	}
	log.Printf("Worker[%d]: Got new Job: %s", this.WorkerId, job.Body)
	// Check for valid url
	uri := string(job.Body[:])
	u, err := url.Parse(uri)
	if err != nil {
		log.Printf(err.Error())
		this.Beanstalkd.Delete(job.Id)
		return err
	}

	if !u.IsAbs() {
		log.Printf("Worker[%d]: Invalid URL: \"%s\". Ignoring message.", this.WorkerId, u)
		this.Beanstalkd.Delete(job.Id)
		return nil
	}

	product, err := scraping.GetProductData(uri)

	if err != nil {
		this.Beanstalkd.Delete(job.Id)
		return err
	}

	productJson, err := json.Marshal(product)

	if err != nil {
		log.Printf("Worker[%d]: Error marshaling product %s: %s", this.WorkerId, product, err.Error())
	}

	var jsonStr = []byte(productJson)

	msgId, e := this.Beanstalkd.Put(0, 0, 0, jsonStr)
	if err != nil {
		log.Printf("Worker[%d]: Error sending product json: %s", this.WorkerId, err.Error())
		return err
	}

	this.Beanstalkd.Delete(job.Id)
	log.Printf("Worker[%d]: Job %d, %s, new product message (%d)", this.WorkerId, job.Id, product.Name, msgId)
	this.Stats.totalFetched++

	return nil
}

func (this *Worker) run() {
	var errors = 0
	for {
		err := this.processMessage()
		// Error management
		if err != nil {
			if oe, ok := err.(*net.OpError); ok && (oe.Err == syscall.EPIPE || oe.Err == syscall.ECONNRESET) {
				log.Printf("Worker[%d]: Error with pipe. Trying to reconnect.", this.WorkerId)
				for {
					err = this.Connect()
					if err == nil {
						log.Printf("Worker[%d]: Reconnected succesfully!", this.WorkerId)
						break
					} else {
						time.Sleep(time.Second)
					}
				}
			} else {
				errors++
				if errors > 20 {
					log.Printf("Worker[%d]: Too many errors. Aborting worker.", this.WorkerId)
					return
				} else if errors > 2 {
					log.Printf("Worker[%d]: %d secuential errors. Sleeping temporarly", this.WorkerId, errors)
					time.Sleep(time.Second * 5)
				}
			}
		} else {
			errors = 0
		}
	}
}

type GetStats struct {
	totalFetched int64
}

func main() {
	var beanstalkdAddr string
	flag.StringVar(&beanstalkdAddr, "baddr", "0.0.0.0:11300", "beanstalkd address (host:port) to consume urls")
	var numWorkers int
	flag.IntVar(&numWorkers, "workers", 10, "number of (goroutines) workers")
	var watchChannel string
	flag.StringVar(&watchChannel, "watchChannel", "fetch", "api endpoint to post results (host:port)")
	var useChannel string
	flag.StringVar(&useChannel, "putChannel", "products", "api endpoint to post results (host:port)")
	flag.Parse()

	settings := BeanstalkSettings{
		Addr:         beanstalkdAddr,
		WatchChannel: watchChannel,
		UseChannel:   useChannel,
	}

	stats := new(GetStats)
	stats.totalFetched = 0

	log.Printf("Starting %d workers.", numWorkers)

	for w := 1; w <= numWorkers; w++ {
		worker := new(Worker)
		worker.ConnSettings = settings
		worker.WorkerId = w
		worker.Stats = stats
		err := worker.Connect()
		if err != nil {
			log.Printf("Error connecting worker to beanstalkd %s. Exiting.", beanstalkdAddr)
			os.Exit(3)
		}

		go worker.run()
	}

	log.Printf("Started %d workers.", numWorkers)

	var lastTotalFetched int64 = 0

	for {
		time.Sleep(time.Second * 10)
		if stats.totalFetched != lastTotalFetched {
			log.Printf("\n"+
				strings.Repeat("#", 50)+
				"\nTotal fetched: %d.\n"+
				"Total goroutines: %d\n"+
				strings.Repeat("#", 50), stats.totalFetched, runtime.NumGoroutine())
			lastTotalFetched = stats.totalFetched
		}
	}

}
