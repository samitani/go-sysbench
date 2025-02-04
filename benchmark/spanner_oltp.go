package benchmark

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	//"github.com/samitani/go-sysbench/driver"
)

const (
	stmtSpannerPointSelects     = "SELECT c FROM sbtest%d WHERE id=@Id"
	stmtSpannerSimpleRanges     = "SELECT c FROM sbtest%d WHERE id BETWEEN @Begin AND @End"
	stmtSpannerSumRanges        = "SELECT SUM(k) FROM sbtest%d WHERE id BETWEEN @Begin AND @End"
	stmtSpannerOrderRanges      = "SELECT c FROM sbtest%d WHERE id BETWEEN %d AND %d ORDER BY c"
	stmtSpannerDistinctRanges   = "SELECT DISTINCT c FROM sbtest%d WHERE id BETWEEN %d AND %d ORDER BY c"
	stmtSpannerIndexUpdates     = "UPDATE sbtest%d SET k=k+1 WHERE id=%d"
	stmtSpannerNonIndex_updates = "UPDATE sbtest%d SET c=? WHERE id=%d"
	stmtSpannerDeletes          = "DELETE FROM sbtest%d WHERE id=%d"
	stmtSpannerInserts          = "INSERT INTO sbtest%d (id, k, c, pad) VALUES (%d, %d, %s, %s)"
)

type (
	SpannerOpts struct {
		SpannerProjectId  string `long:"spanner-project" description:"Spanner Google Cloud project name"`
		SpannerInstanceId string `long:"spanner-instance" description:"Spanner instance id"`
		SpannerDB         string `long:"spanner-db" description:"Spanner database name" default:"sbtest"`
	}

	SpannerOLTP struct {
		opts *BenchmarkOpts
		db   *spanner.Client
	}
)

func newSpannerOLTP(option *BenchmarkOpts) *SpannerOLTP {
	return &SpannerOLTP{opts: option}
}

func (s *SpannerOLTP) Init(ctx context.Context) error {
	client, err := spanner.NewClient(ctx, s.dsn())
	if err != nil {
		return err
	}
	s.db = client

	return nil
}

func (s *SpannerOLTP) Prepare(ctx context.Context) error {
	err := s.createTable(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (s *SpannerOLTP) getRandTableNum() int {
	return sbRand(1, s.opts.Tables)
}

func (s *SpannerOLTP) Event(ctx context.Context) (reads uint64, writes uint64, others uint64, errors uint64, e error) {
	var tableNum = s.getRandTableNum()
	var numReads uint64

	_, err := s.db.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		for i := 0; i < numPointSelects; i++ {
			stmt := spanner.Statement{
				SQL:    fmt.Sprintf(stmtSpannerPointSelects, tableNum),
				Params: map[string]interface{}{"Id": sbRand(0, s.opts.TableSize)},
			}
			_, err := txn.Query(ctx, stmt).Next()
			if err != nil && err != iterator.Done {
				return err
			}
			numReads += 1
		}
		for i := 0; i < numSimpleRanges; i++ {
			begin := sbRand(0, s.opts.TableSize)
			stmt := spanner.Statement{
				SQL:    fmt.Sprintf(stmtSpannerSimpleRanges, tableNum),
				Params: map[string]interface{}{"Begin": begin, "End": begin + rangeSize - 1},
			}
			_, err := txn.Query(ctx, stmt).Next()
			if err != nil && err != iterator.Done {
				return err
			}
			numReads += 1
		}

		return nil
	})
	return numReads, 0, 0, 0, err
}

func (s *SpannerOLTP) Done() error {
	s.db.Close()
	return nil
}

func (s *SpannerOLTP) dsn() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", s.opts.SpannerProjectId, s.opts.SpannerInstanceId, s.opts.SpannerDB)
}

func (s *SpannerOLTP) createTable(ctx context.Context) error {
	adminClient, err := database.NewDatabaseAdminClient(ctx)

	if err != nil {
		return err
	}
	defer adminClient.Close()

	for tableNum := 1; tableNum <= s.opts.Tables; tableNum++ {
		fmt.Printf("Creating table 'sbtest%d'...\n", tableNum)
		op, err := adminClient.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database: s.dsn(),
			Statements: []string{
				fmt.Sprintf(`CREATE TABLE sbtest%d (
				id INT64 NOT NULL,
				k INT64 NOT NULL DEFAULT(0),
				c STRING(120) NOT NULL DEFAULT(''),
				pad STRING(60) NOT NULL DEFAULT(''),
			) PRIMARY KEY (id)`, tableNum),
			},
		})
		if err != nil {
			return err
		}

		if err := op.Wait(ctx); err != nil {
			return err
		}

		fmt.Printf("Inserting %d records into 'sbtest%d'\n", s.opts.TableSize, tableNum)
		insertValues := []string{}
		for i := 0; i < s.opts.TableSize; i++ {
			insertValues = append(insertValues, fmt.Sprintf(`(%d, %d, "%s", "%s") `, i+1, sbRand(0, s.opts.TableSize), getCValue(), getPadValue()))
		}
		query := fmt.Sprintf("INSERT INTO sbtest%d (id, k, c, pad) VALUES", tableNum) + strings.Join(insertValues, ",")

		_, err = s.db.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			stmt := spanner.Statement{
				SQL: query,
			}
			_, err := txn.Update(ctx, stmt)
			if err != nil {
				return err
			}
			return err
		})
		if err != nil {
			return err
		}

	}

	return nil
}
