package models

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"strings"

	"fmt"
	elastic "github.com/olivere/elastic/v7"
	"golang.org/x/sync/errgroup"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

var client *elastic.Client

type doc struct {
	id     string
	object interface{}
}

func ConnectElastic(host string) {
	var err error
	client, err = elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(host))
	if err != nil {
		log.Fatal("FAIL CONNECT TO ELASTIC AT: ", host)
	}
}

func InsertDocsFromFolder(index, folder string, bulkSize int) {
	rand.Seed(time.Now().UnixNano())
	g, ctx := errgroup.WithContext(context.TODO())

	//TODO: test channels with reference
	docs := make(chan doc)
	begin := time.Now()

	// Goroutine to create documents
	g.Go(func() error {
		defer close(docs)

		files, err := ioutil.ReadDir(folder)
		if err != nil {
			return err
		}
		for _, f := range files {
			if !strings.Contains(f.Name(), ".json") {
				continue
			}
			id := strings.TrimRight(f.Name(), ".json")
			object, err := fileToObject(folder + "/" + f.Name())
			if err != nil {
				return err
			}
			// Send over to 2nd goroutine, or cancel
			select {
			case docs <- doc{id: id, object: object}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	// Second goroutine will consume the documents sent from the first and bulk insert into ES
	var total uint64
	g.Go(func() error {
		bulk := client.Bulk().Index(index).Type("doc")
		for doc := range docs {
			// Simple progress
			current := atomic.AddUint64(&total, 1)
			dur := time.Since(begin).Seconds()
			sec := int(dur)
			pps := int64(float64(current) / dur)
			fmt.Printf("%10d | %6d req/s | %02d:%02d\r", current, pps, sec/60, sec%60)

			// Enqueue the document
			bulk.Add(elastic.NewBulkIndexRequest().Id(doc.id).Doc(doc.object))
			if bulk.NumberOfActions() >= bulkSize {
				// Commit
				res, err := bulk.Do(ctx)
				if err != nil {
					return err
				}
				if res.Errors {
					// Look up the failed documents with res.Failed(), and e.g. recommit
					return errors.New("bulk commit failed")
				}
				// "bulk" is reset after Do, so you can reuse it
			}

			select {
			default:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Commit the final batch before exiting
		if bulk.NumberOfActions() > 0 {
			_, err := bulk.Do(ctx)
			if err != nil {
				return err
			}
		}
		return nil
	})

	// Wait until all goroutines are finished
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}

	// Final results
	dur := time.Since(begin).Seconds()
	sec := int(dur)
	pps := int64(float64(total) / dur)
	fmt.Printf("%10d | %6d req/s | %02d:%02d\n", total, pps, sec/60, sec%60)
}

func SaveDocsToFolder(index, folder string, size int) {
	if client != nil {
		total := 9999
		from := 0
		for from+size < total {
			searchResult, err := client.Search().
				Index(index).Query(elastic.NewMatchAllQuery()).
				From(from).Size(size).Pretty(false).Do(context.Background())
			if err != nil {
				log.Println(err)
				time.Sleep(10 * time.Second)
			}

			for _, hit := range searchResult.Hits.Hits {
				data, er := hit.Source.MarshalJSON()
				if er != nil {
					saveErr := ioutil.WriteFile(folder+"/"+hit.Id+".json", data, 0644)
					if saveErr != nil {
						log.Println(err)
					}
				}
			}

			total = int(searchResult.TotalHits())
			from += size
		}
	}
}

func fileToObject(filename string) (interface{}, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	byteValue, _ := ioutil.ReadAll(file)
	var object interface{}
	err = json.Unmarshal(byteValue, &object)
	if err != nil {
		return nil, err
	}
	return object, nil
}
