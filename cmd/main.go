package main

// TODO later
// 1. PostgreSQL 対応
// 2. Spannerのシナリオの作成
// 3. カスタムシナリオを書く
// 4. 乱数生成をクエリを投げる前段階で行う
// 5. thread fairness の計算
// 6. README を書く
// 7. Context をちゃんと引き回す
// 8. 空振りクエリはotherにカウントする
// 9. PreparedStatement の実装
// 10. banner の実装
// 11. コミットに失敗した場合、Reads/Writes/Other にカウントするかそれとも全部エラーにカウントするか
// 12. Spanner Session Pool の様子を観測する

import (
	"log"

	"github.com/jessevdk/go-flags"

	"github.com/samitani/go-sysbench/subcmds/oltp"
	"github.com/samitani/go-sysbench/subcmds/version"
)

func main() {
	parser := flags.NewParser(nil, flags.HelpFlag|flags.PassDoubleDash)

	if err := versioncmd.RegisterSubCommand(parser); err != nil {
		log.Fatal(err)
	}

	// oltp-read-only and oltp-read-write command
	if err := oltpcmd.RegisterSubCommand(parser); err != nil {
		log.Fatal(err)
	}

	if _, err := parser.Parse(); err != nil {
		log.Fatal(err)
	}

}
