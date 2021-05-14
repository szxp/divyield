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
	schemas := make([]string, 0, len(tickers))
	schemasExists := make([]string, 0, len(tickers))
	schemasMissing := make([]string, 0, len(tickers))

	for _, t := range tickers {
		schemas = append(schemas, schemaName(t))
	}
	//fmt.Printf("schemas: %+v\n", schemas)

	err := execNonTx(ctx, db.DB, func(runner runner) error {
		q := sq.Select("schema_name").
			From("information_schema.schemata").
			Where(sq.Eq{"schema_name": schemas}).
			PlaceholderFormat(sq.Dollar)

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
			var schema string
			err = rows.Scan(&schema)
			if err != nil {
				return err
			}
			schemasExists = append(schemasExists, schema)
		}

		return nil
	})

	//fmt.Println("schemas exists: ", schemasExists)

OUTER_LOOP:
	for _, schema := range schemas {
		for _, schemaExists := range schemasExists {
			if schema == schemaExists {
				continue OUTER_LOOP
			}
		}
		schemasMissing = append(schemasMissing, schema)
	}

	//fmt.Println("schemas missing: ", schemasMissing)
	if len(schemasMissing) > 0 {
		tmpl := template.Must(template.New("init").Parse(initSchemaTmpl))

		for _, schema := range schemasMissing {
			err := execTx(ctx, db.DB, func(runner runner) error {
				params := map[string]string{"Schema": schema}
				buf := &bytes.Buffer{}
				err := tmpl.Execute(buf, params)
				if err != nil {
					return err
				}
				_, err = runner.ExecContext(ctx, buf.String())
				return err
			})
			if err != nil {
				return err
			}
		}
	}
	return err
}

func schemaName(ticker string) string {
	s := strings.ToLower(ticker)
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, ".", "_")
	return "s_" + s
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
			"date", "symbol", "close", "close_adj", "high",
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
			var closeAdj float64
			var high float64
			var low float64
			var open float64
			var volume float64

			err = rows.Scan(&date, &symbol, &close, &closeAdj, &high,
				&low, &open, &volume)
			if err != nil {
				return err
			}
			np := &divyield.Price{
				Date:     date,
				Symbol:   symbol,
				Close:    close,
				CloseAdj: closeAdj,
				High:     high,
				Low:      low,
				Open:     open,
				Volume:   volume,
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

		err = updateCloseAdj(ctx, runner, schemaName)
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
			"ex_date", "amount", "amount_adj", "currency",
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
			q = q.Where(sq.Eq{"payment_type": []string{"Cash", "Cash&Stock"}})
		}

		if f.Regular {
			q = q.Where("frequency > ?", 0)
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
			var amountAdj float64
			var currency string
			var frequency int
			var symbol string
			var paymentType string

			err = rows.Scan(&exDate, &amount, &amountAdj, &currency,
				&frequency, &symbol, &paymentType)
			if err != nil {
				return err
			}
			v := &divyield.Dividend{
				ExDate:      exDate,
				Amount:      amount,
				AmountAdj:   amountAdj,
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
			schemaName, "dividend", "id", "ex_date", "symbol",
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
				v.ID, v.ExDate, v.Symbol, v.Amount,
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

		err = updateDividendAdj(ctx, runner, schemaName)
		if err != nil {
			return err
		}

		err = updateCloseAdj(ctx, runner, schemaName)
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
			"close_adj",
			"coalesce((select amount_adj from "+schemaName+".dividend where ex_date <= date and payment_type in ('Cash', 'Cash&Stock') and frequency > 0 order by ex_date desc limit 1), 0) as div_amount_adj",
			"coalesce((select frequency from "+schemaName+".dividend where ex_date <= date and payment_type in ('Cash', 'Cash&Stock') and frequency > 0 order by ex_date desc limit 1), 0) as div_freq",
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

		//fmt.Println(sql)

		rows, err := runner.QueryContext(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var date time.Time
			var closeAdj float64
			var dividendAdj float64
			var frequency int

			err = rows.Scan(&date, &closeAdj, &dividendAdj, &frequency)
			if err != nil {

				fmt.Println("err: ", date.Format(divyield.DateFormat))
				return err
			}
			v := &divyield.DividendYield{
				Date:        date,
				CloseAdj:    closeAdj,
				DividendAdj: dividendAdj,
				Frequency:   frequency,
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

func (db *DB) PrependSplits(
	ctx context.Context,
	ticker string,
	splits []*divyield.Split,
) error {
	if len(splits) == 0 {
		return nil
	}

	err := execTx(ctx, db.DB, func(runner runner) error {
		schemaName := schemaName(ticker)

		var date time.Time
		err := runner.QueryRowContext(ctx,
			"select ex_date from "+schemaName+".split "+
				"order by ex_date desc limit 1").
			Scan(&date)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		if !date.IsZero() {
			newBottom := splits[len(splits)-1]
			if !newBottom.ExDate.Equal(date) {
				return fmt.Errorf(
					"non-overlapping split ex dates %v vs %v",
					newBottom.ExDate, date)
			}

			// forget the last one
			splits = splits[:len(splits)-1]
		}

		stmt, err := runner.PrepareContext(ctx, pq.CopyInSchema(
			schemaName, "split", "ex_date", "to_factor", "from_factor"))
		if err != nil {
			return err
		}

		for _, v := range splits {
			select {
			case <-ctx.Done():
				return fmt.Errorf("interrupted")
			default:
				// noop
			}

			_, err = stmt.ExecContext(
				ctx, v.ExDate, v.ToFactor, v.FromFactor)
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

		err = updateDividendAdj(ctx, runner, schemaName)
		if err != nil {
			fmt.Println("ERROR", err)
			return err
		}

		err = updateCloseAdj(ctx, runner, schemaName)
		if err != nil {
			fmt.Println("ERROR", err)
			return err
		}

		return nil
	})
	return err
}

func updateDividendAdj(
	ctx context.Context,
	runner runner,
	schemaName string,
) error {
	_, err := runner.ExecContext(
		ctx, "call public.update_dividend_adj($1);", schemaName)
	return err
}

func updateCloseAdj(
	ctx context.Context,
	runner runner,
	schemaName string,
) error {
	_, err := runner.ExecContext(
		ctx, "call public.update_price_adj($1);", schemaName)
	return err
}

func (db *DB) Splits(
	ctx context.Context,
	ticker string,
	f *divyield.SplitFilter,
) ([]*divyield.Split, error) {
	splits := make([]*divyield.Split, 0)

	err := execNonTx(ctx, db.DB, func(runner runner) error {
		schemaName := schemaName(ticker)

		q := sq.Select(
			"ex_date", "to_factor", "from_factor").
			From(schemaName + ".split").
			OrderBy("ex_date desc").
			PlaceholderFormat(sq.Dollar)

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
			var exDate time.Time
			var toFactor int
			var fromFactor int

			err = rows.Scan(&exDate, &toFactor, &fromFactor)
			if err != nil {
				return err
			}
			v := &divyield.Split{
				ExDate:     exDate,
				ToFactor:   toFactor,
				FromFactor: fromFactor,
			}
			splits = append(splits, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return splits, nil
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
create schema {{.Schema}};

create table {{.Schema}}.price (
    date        date not null,
    symbol      varchar(10) not null,
    close       numeric not null,
    high        numeric not null,
    low         numeric not null,
    open        numeric not null,
    volume      numeric not null,
    factor_adj  numeric not null default 1,
    close_adj   numeric not null default 0,
    PRIMARY KEY(date)	
);

create table {{.Schema}}.dividend (
    id           bigint not null,
    ex_date      date not null,
    symbol       varchar(10) not null,
    amount       numeric not null,
    currency     char(3) not null,
    frequency    smallint not null,
    payment_type text not null, 
    factor_adj   numeric not null default 1,
    amount_adj   numeric not null default 0,
    PRIMARY KEY(id)	
);

create table {{.Schema}}.split (
    ex_date      date not null,
    to_factor     numeric not null,
    from_factor   numeric not null,
    PRIMARY KEY(ex_date)	
);
`
