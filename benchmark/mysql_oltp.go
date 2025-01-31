package benchmark

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

const (
	stmtPointSelects     = "SELECT c FROM sbtest%d WHERE id=%d"
	stmtSimpleRanges     = "SELECT c FROM sbtest%d WHERE id BETWEEN %d AND %d"
	stmtSumRanges        = "SELECT SUM(k) FROM sbtest%d WHERE id BETWEEN %d AND %d"
	stmtOrderRanges      = "SELECT c FROM sbtest%d WHERE id BETWEEN %d AND %d ORDER BY c"
	stmtDistinctRanges   = "SELECT DISTINCT c FROM sbtest%d WHERE id BETWEEN %d AND %d ORDER BY c"
	stmtIndexUpdates     = "UPDATE sbtest%d SET k=k+1 WHERE id=%d"
	stmtNonIndex_updates = "UPDATE sbtest%d SET c=%s WHERE id=%d"
	stmtDeletes          = "DELETE FROM sbtest%d WHERE id=%d"
	stmtInserts          = "INSERT INTO sbtest%d (id, k, c, pad) VALUES (%d, %d, %s, %s)"

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
)

type (
	MySQLOpts struct {
		MySQLHost     string `long:"mysql-host" description:"MySQL server host" default:"localhost"`
		MySQLPort     int    `long:"mysql-port" description:"MySQL server port" default:"3306"`
		MySQLUser     string `long:"mysql-user" description:"MySQL user" default:"sbtest"`
		MySQLPassword string `long:"mysql-password" env:"MYSQL_PWD" description:"MySQL password" default:""`
		MySQLDB       string `long:"mysql-db" description:"MySQL database name" default:"sbtest"`
	}

	MySQLOLTP struct {
		opts *BenchmarkOpts

		db *sql.DB
	}
)

func newMySQLOLTP(option *BenchmarkOpts) *MySQLOLTP {
	return &MySQLOLTP{opts: option}
}

func (o *MySQLOLTP) Init() error {
	db, err := sql.Open("mysql", o.dsn())
	if err != nil {
		return err
	}

	o.db = db

	return nil
}

func (o *MySQLOLTP) Prepare() error {
	err := o.createTable()
	if err != nil {
		return err
	}
	return nil
}

func (o *MySQLOLTP) Event() (reads uint64, writes uint64, others uint64, errors uint64, e error) {
	var numReads, numWrites, numOthers uint64
	var tableNum = o.getRandTableNum()
	var numRowReturn = 0

	tx, err := o.db.Begin()

	if err != nil {
		return 0, 0, 0, 1, err
	}
	numOthers += 1

	for i := 0; i < numPointSelects; i++ {
		rows, err := tx.Query(fmt.Sprintf(stmtPointSelects, tableNum, sbRand(0, o.opts.TableSize)))
		if err != nil {
			tx.Rollback()
			return numReads, numWrites, numOthers, 1, err
		}
		for rows.Next() {
			numRowReturn += 1
		}
		numReads += 1
	}

	for i := 0; i < numSimpleRanges; i++ {
		begin := sbRand(0, o.opts.TableSize)
		rows, err := tx.Query(fmt.Sprintf(stmtSimpleRanges, tableNum, begin, begin+rangeSize-1))
		if err != nil {
			tx.Rollback()
			return numReads, numWrites, numOthers, 1, err
		}
		for rows.Next() {
			numRowReturn += 1
		}
		numReads += 1
	}

	for i := 0; i < numSumRanges; i++ {
		begin := sbRand(0, o.opts.TableSize)
		rows, err := tx.Query(fmt.Sprintf(stmtSumRanges, tableNum, begin, begin+rangeSize-1))
		if err != nil {
			tx.Rollback()
			return numReads, numWrites, numOthers, 1, err
		}
		for rows.Next() {
			numRowReturn += 1
		}
		numReads += 1
	}

	for i := 0; i < numOrderRanges; i++ {
		begin := sbRand(0, o.opts.TableSize)
		rows, err := tx.Query(fmt.Sprintf(stmtOrderRanges, tableNum, begin, begin+rangeSize-1))
		if err != nil {
			tx.Rollback()
			return numReads, numWrites, numOthers, 1, err
		}
		for rows.Next() {
			numRowReturn += 1
		}
		numReads += 1
	}

	for i := 0; i < numDistinctRanges; i++ {
		begin := sbRand(0, o.opts.TableSize)
		rows, err := tx.Query(fmt.Sprintf(stmtDistinctRanges, tableNum, begin, begin+rangeSize-1))
		if err != nil {
			tx.Rollback()
			return numReads, numWrites, numOthers, 1, err
		}
		for rows.Next() {
			numRowReturn += 1
		}
		numReads += 1
	}

	if o.opts.ReadWrite {
		for i := 0; i < numIndexUpdates; i++ {
			_, err := tx.Exec(fmt.Sprintf(stmtIndexUpdates, tableNum, sbRand(0, o.opts.TableSize)))
			if err != nil {
				tx.Rollback()
				return numReads, numWrites, numOthers, 1, err
			}
			numWrites += 1
		}
		for i := 0; i < numNonIndexUpdates; i++ {
			_, err := tx.Exec(fmt.Sprintf(stmtNonIndex_updates, tableNum, getCValue(), sbRand(0, o.opts.TableSize)))
			if err != nil {
				tx.Rollback()
				return numReads, numWrites, numOthers, 1, err
			}
			numWrites += 1
		}
		for i := 0; i < numDeleteInserts; i++ {
			id := sbRand(0, o.opts.TableSize)

			_, err := tx.Exec(fmt.Sprintf(stmtDeletes, tableNum, id))
			if err != nil {
				tx.Rollback()
				return numReads, numWrites, numOthers, 1, err
			}
			numWrites += 1

			_, err = tx.Exec(fmt.Sprintf(stmtInserts, tableNum, id, sbRand(0, o.opts.TableSize), getCValue(), getPadValue()))
			if err != nil {
				tx.Rollback()
				return numReads, numWrites, numOthers, 1, err
			}
			numWrites += 1
		}

	}

	err = tx.Commit()
	if err != nil {
		return numReads, numWrites, numOthers, 1, err
	}
	numOthers += 1

	return numReads, numWrites, numOthers, 0, nil
}

func (o *MySQLOLTP) Done() error {
	o.db.Close()
	return nil
}

func (o *MySQLOLTP) dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", o.opts.MySQLUser, o.opts.MySQLPassword, o.opts.MySQLHost, o.opts.MySQLPort, o.opts.MySQLDB)
}

func (o *MySQLOLTP) getRandTableNum() int {
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

func (o *MySQLOLTP) createTable() error {
	idDef := "INT NOT NULL AUTO_INCREMENT"
	idIndexDef := "PRIMARY KEY"
	engineDef := ""
	extraTableOptions := ""

	for tableNum := 1; tableNum <= o.opts.Tables; tableNum++ {
		fmt.Printf("Creating table 'sbtest%d'...\n", tableNum)
		query := fmt.Sprintf(`CREATE TABLE sbtest%d(
                                                   id %s,
                                                   k INTEGER DEFAULT '0' NOT NULL,
                                                   c CHAR(120) DEFAULT '' NOT NULL,
                                                   pad CHAR(60) DEFAULT '' NOT NULL,
                                                   %s (id)
                                     ) %s %s`, tableNum, idDef, idIndexDef, engineDef, extraTableOptions)
		_, err := o.db.Exec(query)
		if err != nil {
			return err
		}

		fmt.Printf("Inserting %d records into 'sbtest%d'\n", o.opts.TableSize, tableNum)
		insertValues := []string{}
		for i := 0; i < o.opts.TableSize; i++ {
			insertValues = append(insertValues, fmt.Sprintf(`(%d, "%s", "%s") `, sbRand(0, o.opts.TableSize), getCValue(), getPadValue()))
		}
		query = fmt.Sprintf("INSERT INTO sbtest%d (k, c, pad) VALUES", tableNum) + strings.Join(insertValues, ",")
		_, err = o.db.Exec(query)
		if err != nil {
			return err
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
