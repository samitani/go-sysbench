package main

// TODO
// 1. Spanner Session Pool の様子を観測する
// 2. Spannerのパラメータクエリが有効化できていることを確認する
// 3. SpannerのBIT_REVERSEを試す
// 4. Spannerのシナリオの作成
// 5. SpannerでDeadlineexceeded

// 10. パフォーマンス差分を埋める
// 11. thread fairness の計算


// 31. PostgreSQL の DSN のエスケープ
// 32. 空振りクエリはotherにカウントする
// 33. PostgreSQL SSL ON/OFF

// 41. 乱数生成をクエリを投げる前段階で行う
// 42. PreparedStatement の実装

// 51. カスタムシナリオを書く

// 60. --mysql-ignore-errors のサポート
// 61. MySQL SSL ON/OFF

// 70. README に 日本語を書く
// 71. README に 性能差分について書く
// 72. README に 実行方法について書く

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
