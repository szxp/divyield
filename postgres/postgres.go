package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"text/template"
	"time"
    "github.com/lib/pq"

	"szakszon.com/divyield"
)

type DB struct {
	DB *sql.DB
}

func (db *DB) InitSchema(
	ctx context.Context,
	tickers []string,
) error {
	err := execTx(ctx, db.DB, func(runner runner) error {
		tmpl := template.Must(template.New("init").Parse(initSchemaTmpl))

		for _, ticker := range tickers {
			schemaName := schemaName(ticker)

			var exists bool
			row := runner.QueryRowContext(ctx,
				"select true from information_schema.schemata "+
					"where schema_name = $1;", schemaName)
			err := row.Scan(&exists)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}

			if exists {
				continue
			}

			params := map[string]string{
				"SchemaName": schemaName,
			}
			buf := &bytes.Buffer{}
			err = tmpl.Execute(buf, params)
			if err != nil {
				return err
			}

			_, err = runner.ExecContext(ctx, buf.String())
			return err
		}
		return nil
	})
	return err
}

func schemaName(ticker string) string {
	return "s_" + strings.ToLower(ticker)
}

func (db *DB) Prices(
	ctx context.Context,
	ticker string,
	f *divyield.PriceFilter,
) ([]*divyield.Price, error) {
	return nil, nil
}

func (db *DB) PrependPrices(
	ctx context.Context,
	ticker string,
	prices []*divyield.Price,
) error {
    if len(prices) == 0 {
        return nil
    }

	err := execTx(ctx, db.DB, func(runner runner) error {
		schemaName := schemaName(ticker)

		row := runner.QueryRowContext(ctx,
			"select date from "+schemaName+".price "+
                "order by date desc limit 1")

        var date time.Time
		err := row.Scan(&date)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		if !date.IsZero() {
			newBottom := prices[len(prices)-1]
			if !newBottom.Date.Equal(date) {
				return fmt.Errorf(
					"non-overlapping price dates %v vs %v",
					newBottom.Date, date)
			}

            // forget the last price
            prices = prices[:len(prices)-1]
		}

		stmt, err := runner.PrepareContext(ctx, pq.CopyInSchema(
			schemaName, "price", "date", "symbol",
			"close", "high", "low", "open", "volume"))
		if err != nil {
			return err
		}

		for _, v := range prices {
			select {
			case <-ctx.Done():
				return fmt.Errorf("interrupted")
			default:
				// noop
			}

			_, err = stmt.ExecContext(ctx, v.Date, v.Symbol, v.Close, 
                v.High, v.Low, v.Open, v.Volume)
			if err != nil {
				return err
			}
		}

        _, err = stmt.ExecContext(ctx)
		if err != nil {
			return err
		}

		err = stmt.Close()
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (db *DB) Dividends(
	ctx context.Context,
	ticker string,
	f *divyield.DividendFilter,
) ([]*divyield.Dividend, error) {
	return nil, nil
}

func (db *DB) PrependDividends(
	ctx context.Context,
	ticker string,
	dividends []*divyield.Dividend,
) error {
	return nil
}

type runner interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

func execTx(ctx context.Context, db *sql.DB, fn func(runner runner) error) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}

	err = fn(tx)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}
		return err
	}

	return tx.Commit()
}

const initSchemaTmpl = `
create schema {{.SchemaName}};

create table {{.SchemaName}}.price (
    date        date not null,
    symbol      varchar(10) not null,
    close       numeric not null,
    high        numeric not null,
    low         numeric not null,
    open        numeric not null,
    volume      numeric not null,
    PRIMARY KEY(date)	
);

create table {{.SchemaName}}.dividend (
    ex_date     date not null,
    symbol      varchar(10) not null,
    amount      numeric not null,
    currency    char(3) not null,
    frequency   smallint not null,
    PRIMARY KEY(ex_date)	
);
`
