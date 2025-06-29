package main

import (
	"context"
	"database/sql"
	"fmt"
	"golang.org/x/exp/slices"
	"math/rand"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/googleapis/go-sql-spanner"
	"github.com/lib/pq"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/samitani/go-sysbench"
)

const (
	NameOLTPReadOnly  = "oltp_read_only"
	NameOLTPReadWrite = "oltp_read_write"

	DBDriverMySQL   = "mysql"
	DBDriverPgSQL   = "pgsql"
	DBDriverSpanner = "spanner"

	// https://github.com/akopytov/sysbench/blob/1.0.20/src/lua/oltp_common.lua#L36-L37
	// Range size for range SELECT queries
	rangeSize = 100

	// https://github.com/akopytov/sysbench/blob/1.0.20/src/lua/oltp_common.lua#L40-L41
	// Number of point SELECT queries per transaction
	numPointSelects = 10

	// https://github.com/akopytov/sysbench/blob/1.0.20/src/lua/oltp_common.lua#L42-L43
	// Number of simple range SELECT queries per transaction
	numSimpleRanges = 1

	// https://github.com/akopytov/sysbench/blob/1.0.20/src/lua/oltp_common.lua#L44-L45
	// Number of SELECT SUM() queries per transaction
	numSumRanges = 1

	// https://github.com/akopytov/sysbench/blob/1.0.20/src/lua/oltp_common.lua#L46-L47
	// Number of SELECT ORDER BY queries per transaction
	numOrderRanges = 1

	// https://github.com/akopytov/sysbench/blob/1.0.20/src/lua/oltp_common.lua#L48-L49
	// Number of SELECT DISTINCT queries per transaction
	numDistinctRanges = 1

	// Number of UPDATE index queries per transaction
	numIndexUpdates = 1

	// Number of UPDATE non-index queries per transaction
	numNonIndexUpdates = 1

	// Number of DELETE/INSERT combinations per transaction
	numDeleteInserts = 1

	OptSSLOn  = "on"
	OptSSLOff = "off"

	OptIgnoreErrsAll = "all"

	OptDBPreparedStmtAuto    = "auto"
	OptDBPreparedStmtDisable = "disable"

	rwModeReadOnly  = "ro"
	rwModeReadWrite = "rw"
)

var stmtsMySQL map[string]string = map[string]string{
	"stmtPointSelects":    "SELECT c FROM sbtest%d WHERE id=?",
	"stmtSimpleRanges":    "SELECT c FROM sbtest%d WHERE id BETWEEN ? AND ?",
	"stmtSumRanges":       "SELECT SUM(k) FROM sbtest%d WHERE id BETWEEN ? AND ?",
	"stmtOrderRanges":     "SELECT c FROM sbtest%d WHERE id BETWEEN ? AND ? ORDER BY c",
	"stmtDistinctRanges":  "SELECT DISTINCT c FROM sbtest%d WHERE id BETWEEN ? AND ? ORDER BY c",
	"stmtIndexUpdates":    "UPDATE sbtest%d SET k=k+1 WHERE id=?",
	"stmtNonIndexUpdates": "UPDATE sbtest%d SET c=? WHERE id=?",
	"stmtDeletes":         "DELETE FROM sbtest%d WHERE id=?",
	"stmtInserts":         "INSERT INTO sbtest%d (id, k, c, pad) VALUES (?, ?, ?, ?)",
}

var stmtsPgSQL map[string]string = map[string]string{
	"stmtPointSelects":    "SELECT c FROM sbtest%d WHERE id=$1",
	"stmtSimpleRanges":    "SELECT c FROM sbtest%d WHERE id BETWEEN $1 AND $2",
	"stmtSumRanges":       "SELECT SUM(k) FROM sbtest%d WHERE id BETWEEN $1 AND $2",
	"stmtOrderRanges":     "SELECT c FROM sbtest%d WHERE id BETWEEN $1 AND $2 ORDER BY c",
	"stmtDistinctRanges":  "SELECT DISTINCT c FROM sbtest%d WHERE id BETWEEN $1 AND $2 ORDER BY c",
	"stmtIndexUpdates":    "UPDATE sbtest%d SET k=k+1 WHERE id=$1",
	"stmtNonIndexUpdates": "UPDATE sbtest%d SET c=$1 WHERE id=$2",
	"stmtDeletes":         "DELETE FROM sbtest%d WHERE id=$1",
	"stmtInserts":         "INSERT INTO sbtest%d (id, k, c, pad) VALUES ($1, $2, $3, $4)",
}

type (
	MySQLOpts struct {
		MySQLHost       string `long:"mysql-host" description:"MySQL server host" default:"localhost"`
		MySQLPort       int    `long:"mysql-port" description:"MySQL server port" default:"3306"`
		MySQLUser       string `long:"mysql-user" description:"MySQL user" default:"sbtest"`
		MySQLPassword   string `long:"mysql-password" env:"MYSQL_PWD" description:"MySQL password" default:""`
		MySQLDB         string `long:"mysql-db" description:"MySQL database name" default:"sbtest"`
		MySQLSSL        string `long:"mysql-ssl" choice:"on" choice:"off" description:"use SSL connections" default:"off"` //nolint:staticcheck
		MySQLIgnoreErrs string `long:"mysql-ignore-errors" description:"list of errors to ignore, or \"all\"" default:"1213,1020,1205"`
	}

	PgSQLOpts struct {
		PgSQLHost       string `long:"pgsql-host" description:"PostgreSQL server host" default:"localhost"`
		PgSQLPort       int    `long:"pgsql-port" description:"PostgreSQL server port" default:"5432"`
		PgSQLUser       string `long:"pgsql-user" description:"PostgreSQL user" default:"sbtest"`
		PgSQLPassword   string `long:"pgsql-password" env:"PGPASSWORD" description:"PostgreSQL password" default:""`
		PgSQLDB         string `long:"pgsql-db" description:"PostgreSQL database name" default:"sbtest"`
		PgSQLSSL        string `long:"pgsql-ssl" choice:"on" choice:"off" description:"use SSL connections" default:"off"` //nolint:staticcheck
		PgSQLIgnoreErrs string `long:"pgsql-ignore-errors" description:"list of errors to ignore, or \"all\"" default:"40P01,23505,40001"`
	}

	SpannerOpts struct {
		SpannerProjectId  string `long:"spanner-project" description:"Spanner Google Cloud project name"`
		SpannerInstanceId string `long:"spanner-instance" description:"Spanner instance id"`
		SpannerDB         string `long:"spanner-db" description:"Spanner database name" default:"sbtest"`
	}

	CommonOpts struct {
		Tables         int    `long:"tables" description:"number of tables" default:"1"`
		TableSize      int    `long:"table_size" description:"number of rows per table" default:"10000"`
		TableSizeP     int    `long:"table-size" description:"alias of --table_size"`
		DBDriver       string `long:"db-driver" choice:"mysql" choice:"pgsql" choice:"spanner" description:"specifies database driver to use" default:"mysql"` //nolint:staticcheck
		DBPreparedStmt string `long:"db-ps-mode" choice:"auto" choice:"disable" description:"prepared statements usage mode" default:"auto"`                   //nolint:staticcheck
	}

	BenchmarkOpts struct {
		CommonOpts
		MySQLOpts   `group:"MySQL" description:"MySQL options"`
		PgSQLOpts   `group:"PostgreSQL" description:"PostgreSQL options"`
		SpannerOpts `group:"Spanner" description:"Google Cloud Spanner options"`
	}

	OLTPBench struct {
		opts *BenchmarkOpts

		rwMode         string
		ignoreErrSlice []string
		db             *sql.DB
		staticStmts    map[int]map[string]string
		preparedStmts  map[int]map[string]*sql.Stmt // tableNum -> stmtName -> preparedStmt
		eventFuncRef   func(context.Context) (uint64, uint64, uint64, error)
	}
)

func benchmarkFactory(testname string, opt *BenchmarkOpts) (sysbench.Benchmark, error) {
	if opt.TableSizeP != 0 {
		opt.TableSize = opt.TableSizeP
	}

	if testname == NameOLTPReadOnly {
		return newOLTPBench(opt, rwModeReadOnly), nil
	} else if testname == NameOLTPReadWrite {
		return newOLTPBench(opt, rwModeReadWrite), nil
	}
	return nil, fmt.Errorf("Unknown benchmark: %s", testname)

}

func benchmarkNames() []string {
	return []string{NameOLTPReadOnly, NameOLTPReadWrite}
}

func newOLTPBench(option *BenchmarkOpts, mode string) *OLTPBench {
	var ignoreErrors []string

	if option.DBDriver == DBDriverMySQL {
		ignoreErrors = strings.Split(option.MySQLIgnoreErrs, ",")
	} else if option.DBDriver == DBDriverPgSQL {
		ignoreErrors = strings.Split(option.PgSQLIgnoreErrs, ",")
	}

	return &OLTPBench{opts: option, ignoreErrSlice: ignoreErrors, rwMode: mode}
}

func (o *OLTPBench) Init(ctx context.Context) error {
	var drvName string
	var dsn string

	if o.opts.DBDriver == DBDriverMySQL {
		drvName = "mysql"
		dsn = o.dsnMySQL()
	} else if o.opts.DBDriver == DBDriverPgSQL {
		drvName = "postgres"
		dsn = o.dsnPgSQL()
	} else if o.opts.DBDriver == DBDriverSpanner {
		drvName = "spanner"
		dsn = o.dsnSpanner()
	} else {
		panic("Unexpected driver")
	}

	db, err := sql.Open(drvName, dsn)
	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return err
	}

	o.db = db

	return nil
}

func (o *OLTPBench) PreEvent(ctx context.Context) error {
	var stmtTemplates map[string]string

	if o.opts.DBDriver == DBDriverMySQL {
		stmtTemplates = stmtsMySQL
	} else if o.opts.DBDriver == DBDriverPgSQL {
		stmtTemplates = stmtsPgSQL
	} else if o.opts.DBDriver == DBDriverSpanner {
		stmtTemplates = stmtsMySQL
	} else {
		panic("Unexpected driver")
	}

	var err error
	if o.opts.DBPreparedStmt == OptDBPreparedStmtDisable {
		o.staticStmts = make(map[int]map[string]string)
		for tableNum := 1; tableNum <= o.opts.Tables; tableNum++ {
			o.staticStmts[tableNum] = make(map[string]string)
			for stmtName, stmtString := range stmtTemplates {
				o.staticStmts[tableNum][stmtName] = fmt.Sprintf(stmtString, tableNum)
				if err != nil {
					return err
				}
			}
		}
		o.eventFuncRef = o.eventFuncStaticStmt()
	} else {
		o.preparedStmts = make(map[int]map[string]*sql.Stmt)
		for tableNum := 1; tableNum <= o.opts.Tables; tableNum++ {
			o.preparedStmts[tableNum] = make(map[string]*sql.Stmt)
			for stmtName, stmtString := range stmtTemplates {
				o.preparedStmts[tableNum][stmtName], err = o.db.PrepareContext(ctx, fmt.Sprintf(stmtString, tableNum))
				if err != nil {
					return err
				}
			}
		}
		o.eventFuncRef = o.eventFuncPreparedStmt()
	}
	return nil
}

func (o *OLTPBench) Prepare(ctx context.Context) error {
	err := o.createTable()
	if err != nil {
		return err
	}
	return nil
}

func (o *OLTPBench) Event(ctx context.Context) (numReads, numWrites, numOthers, numIgnoredErros uint64, err error) {
	numReads, numWrites, numOthers, err = o.eventFuncRef(ctx)

	if err != nil {
		if o.opts.DBDriver == DBDriverMySQL {
			me, ok := err.(*mysql.MySQLError)
			if ok {
				// ignore mysql error
				if o.opts.MySQLIgnoreErrs == OptIgnoreErrsAll || slices.Contains(o.ignoreErrSlice, strconv.Itoa(int(me.Number))) {
					return numReads, numWrites, numOthers, 1, nil
				}
			}
		} else if o.opts.DBDriver == DBDriverPgSQL {
			pe, ok := err.(*pq.Error)
			if ok {
				if o.opts.PgSQLIgnoreErrs == OptIgnoreErrsAll || slices.Contains(o.ignoreErrSlice, string(pe.Code)) {
					return numReads, numWrites, numOthers, 1, nil
				}
			}
		} else if o.opts.DBDriver == DBDriverSpanner {
			// ignore spanner concurrent error
			if spannerdriver.ErrAbortedDueToConcurrentModification == err {
				return numReads, numWrites, numOthers, 1, nil
			}

			// convert spanner specific context error to general error
			s, ok := status.FromError(err)
			if ok {
				if s.Code() == codes.DeadlineExceeded {
					err = context.DeadlineExceeded
				} else if s.Code() == codes.Canceled {
					err = context.Canceled
				}
			}
		}
	}

	return numReads, numWrites, numOthers, 0, err
}

func (o *OLTPBench) Done() error {
	o.db.Close()
	return nil
}

func (o *OLTPBench) dsnMySQL() string {
	var tlsParam string
	if o.opts.MySQLSSL == OptSSLOn {
		tlsParam = "skip-verify"
	} else {
		tlsParam = "false"
	}

	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?tls=%s&interpolateParams=%s", o.opts.MySQLUser, o.opts.MySQLPassword, o.opts.MySQLHost, o.opts.MySQLPort, o.opts.MySQLDB, tlsParam, "true")
}

func (o *OLTPBench) dsnPgSQL() string {
	var sslParam string
	if o.opts.PgSQLSSL == OptSSLOn {
		sslParam = "require"
	} else {
		sslParam = "disable"
	}
	return fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s sslmode=%s", o.opts.PgSQLUser, o.opts.PgSQLPassword, o.opts.PgSQLHost, o.opts.PgSQLPort, o.opts.PgSQLDB, sslParam)
}

func (o *OLTPBench) eventFuncStaticStmt() func(context.Context) (uint64, uint64, uint64, error) {
	var txOpt *sql.TxOptions
	if o.opts.DBDriver == DBDriverSpanner && o.rwMode == rwModeReadOnly {
		txOpt = &sql.TxOptions{ReadOnly: true}
	} else {
		txOpt = &sql.TxOptions{}
	}

	return func(ctx context.Context) (numReads, numWrites, numOthers uint64, err error) {
		var tableNum = o.getRandTableNum()

		tx, err := o.db.BeginTx(ctx, txOpt)

		if err != nil {
			return numReads, numWrites, numOthers, err
		}
		numOthers += 1

		for i := 0; i < numPointSelects; i++ {
			rows, err := tx.QueryContext(ctx, o.staticStmts[tableNum]["stmtPointSelects"], sbRand(1, o.opts.TableSize))
			if err != nil {
				_ = tx.Rollback()
				return numReads, numWrites, numOthers, err
			}
			defer rows.Close()
			for rows.Next() {
			}
			numReads += 1
		}

		for i := 0; i < numSimpleRanges; i++ {
			begin := sbRand(1, o.opts.TableSize)
			rows, err := tx.QueryContext(ctx, o.staticStmts[tableNum]["stmtSimpleRanges"], begin, begin+rangeSize-1)
			if err != nil {
				_ = tx.Rollback()
				return numReads, numWrites, numOthers, err
			}
			defer rows.Close()
			for rows.Next() {
			}
			numReads += 1
		}
		for i := 0; i < numSumRanges; i++ {
			begin := sbRand(1, o.opts.TableSize)
			rows, err := tx.QueryContext(ctx, o.staticStmts[tableNum]["stmtSumRanges"], begin, begin+rangeSize-1)
			if err != nil {
				_ = tx.Rollback()
				return numReads, numWrites, numOthers, err
			}
			defer rows.Close()
			for rows.Next() {
			}
			numReads += 1
		}

		for i := 0; i < numOrderRanges; i++ {
			begin := sbRand(1, o.opts.TableSize)
			rows, err := tx.QueryContext(ctx, o.staticStmts[tableNum]["stmtOrderRanges"], begin, begin+rangeSize-1)
			if err != nil {
				_ = tx.Rollback()
				return numReads, numWrites, numOthers, err
			}
			defer rows.Close()
			for rows.Next() {
			}
			numReads += 1
		}

		for i := 0; i < numDistinctRanges; i++ {
			begin := sbRand(1, o.opts.TableSize)
			rows, err := tx.QueryContext(ctx, o.staticStmts[tableNum]["stmtDistinctRanges"], begin, begin+rangeSize-1)
			if err != nil {
				_ = tx.Rollback()
				return numReads, numWrites, numOthers, err
			}
			defer rows.Close()
			for rows.Next() {
			}
			numReads += 1
		}

		if o.rwMode == rwModeReadWrite {
			for i := 0; i < numIndexUpdates; i++ {
				_, err := tx.ExecContext(ctx, o.staticStmts[tableNum]["stmtIndexUpdates"], sbRand(1, o.opts.TableSize))
				if err != nil {
					_ = tx.Rollback()
					return numReads, numWrites, numOthers, err
				}
				numWrites += 1
			}
			for i := 0; i < numNonIndexUpdates; i++ {
				_, err := tx.ExecContext(ctx, o.staticStmts[tableNum]["stmtNonIndexUpdates"], getCValue(), sbRand(1, o.opts.TableSize))
				if err != nil {
					_ = tx.Rollback()
					return numReads, numWrites, numOthers, err
				}
				numWrites += 1
			}
			for i := 0; i < numDeleteInserts; i++ {
				id := sbRand(1, o.opts.TableSize)

				_, err := tx.ExecContext(ctx, o.staticStmts[tableNum]["stmtDeletes"], id)
				if err != nil {
					_ = tx.Rollback()
					return numReads, numWrites, numOthers, err
				}
				numWrites += 1

				_, err = tx.ExecContext(ctx, o.staticStmts[tableNum]["stmtInserts"], id, sbRand(1, o.opts.TableSize), getCValue(), getPadValue())
				if err != nil {
					_ = tx.Rollback()
					return numReads, numWrites, numOthers, err
				}
				numWrites += 1
			}

		}

		err = tx.Commit()
		if err != nil {
			return numReads, numWrites, numOthers, err
		}
		numOthers += 1

		return numReads, numWrites, numOthers, nil
	}
}

func (o *OLTPBench) eventFuncPreparedStmt() func(context.Context) (uint64, uint64, uint64, error) {
	var txOpt *sql.TxOptions
	if o.opts.DBDriver == DBDriverSpanner && o.rwMode == rwModeReadOnly {
		txOpt = &sql.TxOptions{ReadOnly: true}
	} else {
		txOpt = &sql.TxOptions{}
	}

	return func(ctx context.Context) (numReads, numWrites, numOthers uint64, err error) {
		var tableNum = o.getRandTableNum()

		tx, err := o.db.BeginTx(ctx, txOpt)

		if err != nil {
			return numReads, numWrites, numOthers, err
		}
		numOthers += 1

		for i := 0; i < numPointSelects; i++ {
			rows, err := tx.Stmt(o.preparedStmts[tableNum]["stmtPointSelects"]).QueryContext(ctx, sbRand(1, o.opts.TableSize))
			if err != nil {
				_ = tx.Rollback()
				return numReads, numWrites, numOthers, err
			}
			defer rows.Close()
			for rows.Next() {
			}
			numReads += 1
		}

		for i := 0; i < numSimpleRanges; i++ {
			begin := sbRand(1, o.opts.TableSize)
			rows, err := tx.Stmt(o.preparedStmts[tableNum]["stmtSimpleRanges"]).QueryContext(ctx, begin, begin+rangeSize-1)
			if err != nil {
				_ = tx.Rollback()
				return numReads, numWrites, numOthers, err
			}
			defer rows.Close()
			for rows.Next() {
			}
			numReads += 1
		}

		for i := 0; i < numSumRanges; i++ {
			begin := sbRand(1, o.opts.TableSize)
			rows, err := tx.Stmt(o.preparedStmts[tableNum]["stmtSumRanges"]).QueryContext(ctx, begin, begin+rangeSize-1)
			if err != nil {
				_ = tx.Rollback()
				return numReads, numWrites, numOthers, err
			}
			defer rows.Close()
			for rows.Next() {
			}
			numReads += 1
		}

		for i := 0; i < numOrderRanges; i++ {
			begin := sbRand(1, o.opts.TableSize)
			rows, err := tx.Stmt(o.preparedStmts[tableNum]["stmtOrderRanges"]).QueryContext(ctx, begin, begin+rangeSize-1)
			if err != nil {
				_ = tx.Rollback()
				return numReads, numWrites, numOthers, err
			}
			defer rows.Close()
			for rows.Next() {
			}
			numReads += 1
		}

		for i := 0; i < numDistinctRanges; i++ {
			begin := sbRand(1, o.opts.TableSize)
			rows, err := tx.Stmt(o.preparedStmts[tableNum]["stmtDistinctRanges"]).QueryContext(ctx, begin, begin+rangeSize-1)
			if err != nil {
				_ = tx.Rollback()
				return numReads, numWrites, numOthers, err
			}
			defer rows.Close()
			for rows.Next() {
			}
			numReads += 1
		}

		if o.rwMode == rwModeReadWrite {
			for i := 0; i < numIndexUpdates; i++ {
				res, err := tx.Stmt(o.preparedStmts[tableNum]["stmtIndexUpdates"]).ExecContext(ctx, sbRand(1, o.opts.TableSize))
				if err != nil {
					_ = tx.Rollback()
					return numReads, numWrites, numOthers, err
				}
				rows, err := res.RowsAffected()
				if err != nil {
					return numReads, numWrites, numOthers, err
				}
				if rows == 0 {
					numOthers += 1
				} else {
					numWrites += 1
				}
			}
			for i := 0; i < numNonIndexUpdates; i++ {
				res, err := tx.Stmt(o.preparedStmts[tableNum]["stmtNonIndexUpdates"]).ExecContext(ctx, getCValue(), sbRand(1, o.opts.TableSize))
				if err != nil {
					_ = tx.Rollback()
					return numReads, numWrites, numOthers, err
				}
				rows, err := res.RowsAffected()
				if err != nil {
					return numReads, numWrites, numOthers, err
				}
				if rows == 0 {
					numOthers += 1
				} else {
					numWrites += 1
				}
			}
			for i := 0; i < numDeleteInserts; i++ {
				id := sbRand(1, o.opts.TableSize)

				res, err := tx.Stmt(o.preparedStmts[tableNum]["stmtDeletes"]).ExecContext(ctx, id)
				if err != nil {
					_ = tx.Rollback()
					return numReads, numWrites, numOthers, err
				}
				rows, err := res.RowsAffected()
				if err != nil {
					return numReads, numWrites, numOthers, err
				}
				if rows == 0 {
					numOthers += 1
				} else {
					numWrites += 1
				}

				res, err = tx.Stmt(o.preparedStmts[tableNum]["stmtInserts"]).ExecContext(ctx, id, sbRand(1, o.opts.TableSize), getCValue(), getPadValue())
				if err != nil {
					_ = tx.Rollback()
					return numReads, numWrites, numOthers, err
				}
				rows, err = res.RowsAffected()
				if err != nil {
					return numReads, numWrites, numOthers, err
				}
				if rows == 0 {
					numOthers += 1
				} else {
					numWrites += 1
				}
			}

		}

		err = tx.Commit()
		if err != nil {
			return numReads, numWrites, numOthers, err
		}
		numOthers += 1

		return numReads, numWrites, numOthers, nil
	}
}

func (o *OLTPBench) dsnSpanner() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", o.opts.SpannerProjectId, o.opts.SpannerInstanceId, o.opts.SpannerDB)
}

func (o *OLTPBench) getRandTableNum() int {
	return sbRand(1, o.opts.Tables)
}

func getCValue() string {
	// 10 groups, 119 characters
	return sbRandStr("###########-###########-###########-###########-###########-###########-###########-###########-###########-###########")
}

func getPadValue() string {
	return sbRandStr("###########-###########-###########-###########-###########")
}

func sbRand(minimum int, maximum int) int {
	return rand.Intn(maximum-minimum+1) + minimum
}

func sbRandStr(format string) string {
	buf := make([]rune, len(format))
	for i, c := range format {
		if c == '#' {
			buf[i] = rune(sbRand(int('0'), int('9')))
		} else if c == '@' {
			buf[i] = rune(sbRand(int('a'), int('z')))
		} else {
			buf[i] = c
		}
	}

	return string(buf)
}

func (o *OLTPBench) createTable() error {
	var idDef string

	if o.opts.DBDriver == DBDriverPgSQL {
		idDef = "INT NOT NULL"
	} else {
		idDef = "INT NOT NULL AUTO_INCREMENT"
	}

	idIndexDef := "PRIMARY KEY"

	for tableNum := 1; tableNum <= o.opts.Tables; tableNum++ {
		fmt.Printf("Creating table 'sbtest%d'...\n", tableNum)
		var query string

		if o.opts.DBDriver == DBDriverSpanner {
			query = fmt.Sprintf(`CREATE TABLE sbtest%d (
				id INT64 NOT NULL,
				k INT64 NOT NULL DEFAULT(0),
				c STRING(120) NOT NULL DEFAULT(''),
				pad STRING(60) NOT NULL DEFAULT(''),
			) PRIMARY KEY (id)`, tableNum)
		} else {
			query = fmt.Sprintf(`CREATE TABLE sbtest%d(
                                                   id %s,
                                                   k INTEGER DEFAULT '0' NOT NULL,
                                                   c CHAR(120) DEFAULT '' NOT NULL,
                                                   pad CHAR(60) DEFAULT '' NOT NULL,
                                                   %s (id)
                                     )`, tableNum, idDef, idIndexDef)
		}

		_, err := o.db.Exec(query)
		if err != nil {
			return err
		}

		fmt.Printf("Inserting %d records into 'sbtest%d'\n", o.opts.TableSize, tableNum)
		insertValues := []string{}
		for i := 1; i <= o.opts.TableSize; i++ {
			insertValues = append(insertValues, fmt.Sprintf(`(%d, %d, '%s', '%s') `, i, sbRand(1, o.opts.TableSize), getCValue(), getPadValue()))

			// Spanner max query size is 1M
			// https://cloud.google.com/spanner/quotas#query-limits
			if i%500 == 0 {
				query = fmt.Sprintf("INSERT INTO sbtest%d (id, k, c, pad) VALUES", tableNum) + strings.Join(insertValues, ",")
				_, err = o.db.Exec(query)
				if err != nil {
					return err
				}
				insertValues = []string{}
			}
		}
		if len(insertValues) > 0 {
			query = fmt.Sprintf("INSERT INTO sbtest%d (id, k, c, pad) VALUES", tableNum) + strings.Join(insertValues, ",")
			_, err = o.db.Exec(query)
			if err != nil {
				return err
			}
		}

		fmt.Printf("Creating a secondary index on 'sbtest%d'...\n", tableNum)
		query = fmt.Sprintf("CREATE INDEX k_%d ON sbtest%d(k)", tableNum, tableNum)
		_, err = o.db.Exec(query)
		if err != nil {
			return err
		}
	}

	return nil
}

