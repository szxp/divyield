package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/lib/pq"
	"strings"
	"text/template"
	"time"

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
	prices := make([]*divyield.Price, 0)

	err := execNonTx(ctx, db.DB, func(runner runner) error {
		schemaName := schemaName(ticker)

		q := sq.Select(
			"date", "symbol", "close", "high",
			"low", "open", "volume").
			From(schemaName + ".price").
			OrderBy("date desc").
			PlaceholderFormat(sq.Dollar)

		if !f.From.IsZero() {
			q = q.Where("date >= ?", f.From)
		}

		if f.Limit > 0 {
			q = q.Limit(f.Limit)
		}

		sql, args, err := q.ToSql()
		if err != nil {
			return err
		}

		rows, err := runner.QueryContext(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var date time.Time
			var symbol string
			var close float64
			var high float64
			var low float64
			var open float64
			var volume float64

			err = rows.Scan(&date, &symbol, &close, &high,
				&low, &open, &volume)
			if err != nil {
				return err
			}
			np := &divyield.Price{
				Date:   date,
				Symbol: symbol,
				Close:  close,
				High:   high,
				Low:    low,
				Open:   open,
				Volume: volume,
			}
			prices = append(prices, np)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return prices, nil
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

			// forget the last one
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
	dividends := make([]*divyield.Dividend, 0)

	err := execNonTx(ctx, db.DB, func(runner runner) error {
		schemaName := schemaName(ticker)

		q := sq.Select(
			"ex_date", "amount", "currency",
			"frequency", "symbol", "payment_type").
			From(schemaName + ".dividend").
			OrderBy("ex_date desc").
			PlaceholderFormat(sq.Dollar)

		if !f.From.IsZero() {
			q = q.Where("ex_date >= ?", f.From)
		}

		if f.Limit > 0 {
			q = q.Limit(f.Limit)
		}

		if f.CashOnly {
			q = q.Where("payment_type = ?", "Cash")
		}

		sql, args, err := q.ToSql()
		if err != nil {
			return err
		}

		rows, err := runner.QueryContext(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var exDate time.Time
			var amount float64
			var currency string
			var frequency int
			var symbol string
			var paymentType string

			err = rows.Scan(&exDate, &amount, &currency,
				&frequency, &symbol, &paymentType)
			if err != nil {
				return err
			}
			v := &divyield.Dividend{
				ExDate:      exDate,
				Amount:      amount,
				Currency:    currency,
				Frequency:   frequency,
				Symbol:      symbol,
				PaymentType: paymentType,
			}
			dividends = append(dividends, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return dividends, nil
}

func (db *DB) PrependDividends(
	ctx context.Context,
	ticker string,
	dividends []*divyield.Dividend,
) error {
	if len(dividends) == 0 {
		return nil
	}

	err := execTx(ctx, db.DB, func(runner runner) error {
		schemaName := schemaName(ticker)

		var date time.Time
		err := runner.QueryRowContext(ctx,
			"select ex_date from "+schemaName+".dividend "+
				"order by ex_date desc limit 1").
			Scan(&date)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		if !date.IsZero() {
			newBottom := dividends[len(dividends)-1]
			if !newBottom.ExDate.Equal(date) {
				return fmt.Errorf(
					"non-overlapping dividend ex dates %v vs %v",
					newBottom.ExDate, date)
			}

			// forget the last one
			dividends = dividends[:len(dividends)-1]
		}

		stmt, err := runner.PrepareContext(ctx, pq.CopyInSchema(
			schemaName, "dividend", "ex_date", "symbol",
			"amount", "currency", "frequency", "payment_type"))
		if err != nil {
			return err
		}

		for _, v := range dividends {
			select {
			case <-ctx.Done():
				return fmt.Errorf("interrupted")
			default:
				// noop
			}

			_, err = stmt.ExecContext(ctx,
				v.ExDate, v.Symbol, v.Amount,
				v.Currency, v.Frequency, v.PaymentType)
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

func (db *DB) DividendYields(
	ctx context.Context,
	ticker string,
	f *divyield.DividendYieldFilter,
) ([]*divyield.DividendYield, error) {
	yields := make([]*divyield.DividendYield, 0)

	err := execNonTx(ctx, db.DB, func(runner runner) error {
		schemaName := schemaName(ticker)

		q := sq.Select(
			"date",
			"close",
			"(select coalesce(amount, 0) from "+schemaName+".dividend where ex_date <= date and payment_type = 'Cash' order by ex_date desc limit 1) as div_amount",
			"(select coalesce(frequency, 0) from "+schemaName+".dividend where ex_date <= date and payment_type = 'Cash' order by ex_date desc limit 1) as div_freq",
		).
			From(schemaName + ".price").
			OrderBy("date desc").
			PlaceholderFormat(sq.Dollar)

		if !f.From.IsZero() {
			q = q.Where("date >= ?", f.From)
		}

		if f.Limit > 0 {
			q = q.Limit(f.Limit)
		}

		sql, args, err := q.ToSql()
		if err != nil {
			return err
		}

		rows, err := runner.QueryContext(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var date time.Time
			var close float64
			var dividend float64
			var frequency int

			err = rows.Scan(&date, &close, &dividend, &frequency)
			if err != nil {
				return err
			}
			v := &divyield.DividendYield{
				Date:      date,
				Close:     close,
				Dividend:  dividend,
				Frequency: frequency,
			}
			yields = append(yields, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return yields, nil
}

type runner interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

func execTx(
	ctx context.Context,
	db *sql.DB,
	fn func(runner runner) error,
) error {
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

func execNonTx(
	ctx context.Context,
	db *sql.DB,
	fn func(runner runner) error,
) error {
	return fn(db)
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
    ex_date      date not null,
    symbol       varchar(10) not null,
    amount       numeric not null,
    currency     char(3) not null,
    frequency    smallint not null,
    payment_type text not null, 
    PRIMARY KEY(ex_date)	
);
`
