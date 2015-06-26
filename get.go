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

type BeanstalkSettings struct {
	addr         string
	watchChannel string
	useChannel   string
}

type Worker struct {
	workerId     int
	beanstalkd   *lentil.Beanstalkd
	stats        *GetStats
	connSettings BeanstalkSettings
}

func (this *Worker) Connect() error {

	var e error
	this.beanstalkd, e = lentil.Dial(this.connSettings.addr)
	if e != nil {
		return e
	}

	if this.connSettings.watchChannel != "" {
		_, e = this.beanstalkd.Watch(this.connSettings.watchChannel)
		if e != nil {
			return e
		}
	}

	if this.connSettings.useChannel != "" {
		e = this.beanstalkd.Use(this.connSettings.useChannel)
		if e != nil {
			return e
		}
	}

	return nil
}

func (this *Worker) processMessage() error {
	job, e := this.beanstalkd.Reserve()
	if e != nil {
		log.Printf(e.Error())
		return e
	}
	log.Printf("Worker[%d]: Got new Job: %s", this.workerId, job.Body)
	// Check for valid url
	uri := string(job.Body[:])
	u, err := url.Parse(uri)
	if err != nil {
		log.Printf(err.Error())
		this.beanstalkd.Delete(job.Id)
		return err
	}

	if !u.IsAbs() {
		log.Printf("Worker[%d]: Invalid URL: \"%s\". Ignoring message.", this.workerId, u)
		this.beanstalkd.Delete(job.Id)
		return nil
	}

	product, err := scraping.GetProductData(uri)

	if err != nil {
		if ae, ok := err.(*scraping.ScrapeError); ok {
			if ae.Arg == scraping.SITE_ERROR {
				log.Printf("Worker[%d]: Site error por url: %s", this.workerId, u)
				this.beanstalkd.Delete(job.Id)
				return err
			} else {
				// If INVALID URL or CLIENT ERROR
				log.Printf("Worker[%d]: Invalid Url (%s) or Client error: %s", this.workerId, u, err.Error())
				this.beanstalkd.Delete(job.Id)
				return nil
			}
		}
		this.beanstalkd.Delete(job.Id)
		return err
	}

	productJson, err := json.Marshal(product)

	if err != nil {
		log.Printf("Worker[%d]: Error marshaling product %s: %s", this.workerId, product, err.Error())
		this.beanstalkd.Delete(job.Id)
		return err
	}

	var jsonStr = []byte(productJson)

	msgId, e := this.beanstalkd.Put(0, 0, 0, jsonStr)
	if err != nil {
		log.Printf("Worker[%d]: Error sending product json: %s", this.workerId, err.Error())
		// Do not delete job, trying again.
		return err
	}

	this.beanstalkd.Delete(job.Id)
	log.Printf("Worker[%d]: Job %d, %s, new product message (%d)", this.workerId, job.Id, product.Name, msgId)
	this.stats.totalFetched++

	return nil
}

func (this *Worker) run() {
	var errors = 0
	for {
		err := this.processMessage()
		// Error management
		if err != nil {
			if oe, ok := err.(*net.OpError); ok && (oe.Err == syscall.EPIPE || oe.Err == syscall.ECONNRESET) {
				log.Printf("Worker[%d]: Error with pipe. Trying to reconnect.", this.workerId)
				for {
					err = this.Connect()
					if err == nil {
						log.Printf("Worker[%d]: Reconnected succesfully!", this.workerId)
						break
					} else {
						time.Sleep(time.Second)
					}
				}
			} else {
				errors++
				if errors > 2 {
					log.Printf("Worker[%d]: %d secuential errors. Sleeping temporarly. Error: %s", this.workerId, errors, err.Error())
					time.Sleep(time.Second * time.Duration(errors))
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
	flag.StringVar(&watchChannel, "watchChannel", "fetch", "beanstalk watch channel to receive urls.")
	var useChannel string
	flag.StringVar(&useChannel, "putChannel", "products", "beanstalk use channel to send product jsons.")
	flag.Parse()

	settings := BeanstalkSettings{
		addr:         beanstalkdAddr,
		watchChannel: watchChannel,
		useChannel:   useChannel,
	}

	stats := new(GetStats)
	stats.totalFetched = 0

	log.Printf("Starting %d workers.", numWorkers)

	for w := 1; w <= numWorkers; w++ {
		worker := new(Worker)
		worker.connSettings = settings
		worker.workerId = w
		worker.stats = stats
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
