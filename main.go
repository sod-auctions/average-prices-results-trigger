package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sod-auctions/auctions-db"
	"io"
	"log"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type PriceAverage struct {
	RowCount    int64
	QuantitySum int64
	P05Sum      int64
	P10Sum      int64
	P25Sum      int64
	P50Sum      int64
	P75Sum      int64
	P90Sum      int64
}

func init() {
	log.SetFlags(0)
}

func download(s3Client *s3.S3, ctx aws.Context, record *events.S3EventRecord, key string) (*s3.GetObjectOutput, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(record.S3.Bucket.Name),
		Key:    aws.String(key),
	}

	return s3Client.GetObjectWithContext(ctx, input)
}

func downloadRecent(client *s3.S3, ctx context.Context, bucket string, path string) (*s3.GetObjectOutput, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(path),
	}

	objects, err := client.ListObjectsV2(input)
	if err != nil {
		return nil, err
	}

	// Filter for .csv files and find the most recent one
	var latestKey string
	var latestTime time.Time
	for _, object := range objects.Contents {
		key := *object.Key
		if strings.HasSuffix(key, ".csv") && object.LastModified.After(latestTime) {
			latestKey = key
			latestTime = *object.LastModified
		}
	}

	// If no .csv file was found, return an error
	if latestKey == "" {
		return nil, fmt.Errorf("no .csv file found in path")
	}

	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(latestKey),
	}

	return client.GetObjectWithContext(ctx, getObjectInput)
}

func enrichData(closer io.ReadCloser, priceAverages map[string]*PriceAverage) ([]*auctions_db.PriceAverage, error) {
	priceAveragesDb := make([]*auctions_db.PriceAverage, 0)
	r := csv.NewReader(closer)

	// Read header
	_, err := r.Read()
	if err != nil {
		return nil, err
	}

	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		realmId, _ := strconv.Atoi(row[1])
		auctionHouseId, _ := strconv.Atoi(row[2])
		itemId, _ := strconv.Atoi(row[3])
		quantity, _ := strconv.Atoi(row[4])
		p05, _ := strconv.Atoi(row[7])
		p10, _ := strconv.Atoi(row[8])
		p25, _ := strconv.Atoi(row[9])
		p50, _ := strconv.Atoi(row[10])
		p75, _ := strconv.Atoi(row[11])
		p90, _ := strconv.Atoi(row[12])

		key := fmt.Sprintf("%d-%d-%d", realmId, auctionHouseId, itemId)
		priceAverage, ok := priceAverages[key]
		if !ok {
			return nil, fmt.Errorf("no price average found for key %s", key)
		} else {
			quantityCurrent := int32(quantity)
			quantityAverage := int32(math.Round(float64(priceAverage.QuantitySum) / float64(priceAverage.RowCount)))
			quantityPercent := float32(float64(quantityCurrent) / float64(quantityAverage) * 100)
			p05Current := int32(p05)
			p05Average := int32(math.Round(float64(priceAverage.P05Sum) / float64(priceAverage.RowCount)))
			p05Percent := float32(float64(p05Current) / float64(p05Average) * 100)
			p10Current := int32(p10)
			p10Average := int32(math.Round(float64(priceAverage.P10Sum) / float64(priceAverage.RowCount)))
			p10Percent := float32(float64(p10Current) / float64(p10Average) * 100)
			p25Current := int32(p25)
			p25Average := int32(math.Round(float64(priceAverage.P25Sum) / float64(priceAverage.RowCount)))
			p25Percent := float32(float64(p25Current) / float64(p25Average) * 100)
			p50Current := int32(p50)
			p50Average := int32(math.Round(float64(priceAverage.P50Sum) / float64(priceAverage.RowCount)))
			p50Percent := float32(float64(p50Current) / float64(p50Average) * 100)
			p75Current := int32(p75)
			p75Average := int32(math.Round(float64(priceAverage.P75Sum) / float64(priceAverage.RowCount)))
			p75Percent := float32(float64(p75Current) / float64(p75Average) * 100)
			p90Current := int32(p90)
			p90Average := int32(math.Round(float64(priceAverage.P90Sum) / float64(priceAverage.RowCount)))
			p90Percent := float32(float64(p90Current) / float64(p90Average) * 100)

			priceAveragesDb = append(priceAveragesDb, &auctions_db.PriceAverage{
				RealmID:         int16(realmId),
				AuctionHouseId:  int16(auctionHouseId),
				ItemID:          int32(itemId),
				QuantityCurrent: quantityCurrent,
				QuantityAverage: quantityAverage,
				QuantityPercent: quantityPercent,
				P05Current:      p05Current,
				P05Average:      p05Average,
				P05Percent:      p05Percent,
				P10Current:      p10Current,
				P10Average:      p10Average,
				P10Percent:      p10Percent,
				P25Current:      p25Current,
				P25Average:      p25Average,
				P25Percent:      p25Percent,
				P50Current:      p50Current,
				P50Average:      p50Average,
				P50Percent:      p50Percent,
				P75Current:      p75Current,
				P75Average:      p75Average,
				P75Percent:      p75Percent,
				P90Current:      p90Current,
				P90Average:      p90Average,
				P90Percent:      p90Percent,
			})
		}
	}
	return priceAveragesDb, nil
}

func read(closer io.ReadCloser) (map[string]*PriceAverage, error) {
	priceAverages := make(map[string]*PriceAverage)
	r := csv.NewReader(closer)

	// Read header
	_, err := r.Read()
	if err != nil {
		return nil, err
	}

	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		realmId, _ := strconv.Atoi(row[0])
		auctionHouseId, _ := strconv.Atoi(row[1])
		itemId, _ := strconv.Atoi(row[2])
		rowCount, _ := strconv.Atoi(row[3])
		quantitySum, _ := strconv.Atoi(row[4])
		p05Sum, _ := strconv.Atoi(row[5])
		p10Sum, _ := strconv.Atoi(row[6])
		p25Sum, _ := strconv.Atoi(row[7])
		p50Sum, _ := strconv.Atoi(row[8])
		p75Sum, _ := strconv.Atoi(row[9])
		p90Sum, _ := strconv.Atoi(row[10])

		priceAverage := &PriceAverage{
			RowCount:    int64(rowCount),
			QuantitySum: int64(quantitySum),
			P05Sum:      int64(p05Sum),
			P10Sum:      int64(p10Sum),
			P25Sum:      int64(p25Sum),
			P50Sum:      int64(p50Sum),
			P75Sum:      int64(p75Sum),
			P90Sum:      int64(p90Sum),
		}

		key := fmt.Sprintf("%d-%d-%d", realmId, auctionHouseId, itemId)
		priceAverages[key] = priceAverage
	}
	return priceAverages, nil
}

func handler(ctx context.Context, event events.S3Event) error {
	database, err := auctions_db.NewDatabase(os.Getenv("DB_CONNECTION_STRING"))
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}

	sess := session.Must(session.NewSession())
	s3Client := s3.New(sess)

	record := event.Records[0]
	key, err := url.QueryUnescape(record.S3.Object.Key)
	if err != nil {
		return fmt.Errorf("error decoding S3 object key: %v", err)
	}

	log.Printf("downloading file %s", key)
	file, err := download(s3Client, ctx, &record, key)
	if err != nil {
		return fmt.Errorf("error downloading file: %v", err)
	}
	defer file.Body.Close()

	priceAverages, err := read(file.Body)
	if err != nil {
		return fmt.Errorf("error reading file: %v", err)
	}

	time := time.Now().UTC()
	file, err = downloadRecent(s3Client, ctx, "sod-auctions",
		fmt.Sprintf("results/aggregates/interval=1/year=%s/month=%s/day=%s/hour=%s",
			time.Format("2006"), time.Format("01"), time.Format("02"), time.Format("15")))
	if err != nil {
		return fmt.Errorf("error downloading file: %v", err)
	}

	priceAveragesDb, err := enrichData(file.Body, priceAverages)
	if err != nil {
		return fmt.Errorf("error enriching data: %v", err)
	}

	err = database.ReplacePriceAverages(priceAveragesDb)
	if err != nil {
		return fmt.Errorf("error replacing price averages: %v", err)
	}
	return nil
}

func main() {
	lambda.Start(handler)
}
