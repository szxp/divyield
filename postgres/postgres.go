package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/lib/pq"
	"strings"
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
		schemas = append(schemas, schemaStock(t))
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
		for _, schema := range schemasMissing {
			err := execTx(ctx, db.DB, func(runner runner) error {
				sql := fmt.Sprintf(
					"call public.init_schema_tables('%s');"+
						"call public.init_schema_views('%s');",
					schema,
					schema,
				)
				_, err := runner.ExecContext(ctx, sql)
				return err
			})
			if err != nil {
				return err
			}
		}
	}
	return err
}

func schemaStock(ticker string) string {
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
		schema := schemaStock(ticker)

		q := sq.Select(
			"date",
			"symbol",
			"close",
			"close_adj_splits",
			"high",
			"low",
			"open",
			"volume",
			"currency",
		).
			From(schema + ".price").
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
			var closeAdjSplits float64
			var high float64
			var low float64
			var open float64
			var volume float64
			var currency string

			err = rows.Scan(
				&date,
				&symbol,
				&close,
				&closeAdjSplits,
				&high,
				&low,
				&open,
				&volume,
				&currency,
			)
			if err != nil {
				return err
			}
			np := &divyield.Price{
				Date:           date,
				Symbol:         symbol,
				Close:          close,
				CloseAdjSplits: closeAdjSplits,
				High:           high,
				Low:            low,
				Open:           open,
				Volume:         volume,
				Currency:       currency,
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

func (db *DB) SavePrices(
	ctx context.Context,
	in *divyield.DBSavePricesInput,
) (*divyield.DBSavePricesOutput, error) {
	err := execTx(ctx, db.DB, func(runner runner) error {
		schema := schemaStock(in.Symbol)

		if in.Reset {
			sql, args, err := sq.
				Delete("").
				From(schema + ".price").
				ToSql()

			_, err = runner.ExecContext(ctx, sql, args...)
			if err != nil {
				return err
			}
		}

		stmt, err := runner.PrepareContext(
			ctx,
			pq.CopyInSchema(
				schema,
				"price",
				"date",
				"symbol",
				"close",
				"high",
				"low",
				"open",
				"volume",
				"currency",
				"created",
			),
		)
		if err != nil {
			return err
		}

		for _, v := range in.Prices {
			select {
			case <-ctx.Done():
				return fmt.Errorf("interrupted")
			default:
				// noop
			}

			_, err = stmt.ExecContext(
				ctx,
				v.Date,
				v.Symbol,
				v.Close,
				v.High,
				v.Low,
				v.Open,
				v.Volume,
				v.Currency,
				time.Now(),
			)
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

		err = updateCloseAdj(ctx, runner, schema)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &divyield.DBSavePricesOutput{}, nil
}

func (db *DB) Dividends(
	ctx context.Context,
	ticker string,
	f *divyield.DividendFilter,
) ([]*divyield.Dividend, error) {
	dividends := make([]*divyield.Dividend, 0)

	err := execNonTx(ctx, db.DB, func(runner runner) error {
		schema := schemaStock(ticker)

		q := sq.Select(
			"ex_date",
			"amount",
			"amount_adj",
			"currency",
			"frequency",
			"symbol",
			"payment_type",
			"created",
		).
			From(schema + ".dividend_view").
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
			q = q.Where("frequency > ?", 0)
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
			var created time.Time

			err = rows.Scan(
				&exDate,
				&amount,
				&amountAdj,
				&currency,
				&frequency,
				&symbol,
				&paymentType,
				&created,
			)
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
				Created:     created,
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

func (db *DB) SaveDividends(
	ctx context.Context,
	in *divyield.DBSaveDividendsInput,
) (*divyield.DBSaveDividendsOutput, error) {
	err := execTx(ctx, db.DB, func(runner runner) error {
		schema := schemaStock(in.Symbol)

		if in.Reset {
			sql, args, err := sq.
				Delete("").
				From(schema + ".dividend").
				ToSql()

			_, err = runner.ExecContext(ctx, sql, args...)
			if err != nil {
				return err
			}
		}

		processedIDs, err := db.dividendIDs(ctx, runner, in.Symbol)
		if err != nil {
			return err
		}

		stmt, err := runner.PrepareContext(
			ctx,
			pq.CopyInSchema(
				schema,
				"dividend",
				"id",
				"ex_date",
				"symbol",
				"amount",
				"currency",
				"frequency",
				"payment_type",
				"created",
			),
		)
		if err != nil {
			return err
		}

		for _, v := range in.Dividends {
			select {
			case <-ctx.Done():
				return fmt.Errorf("interrupted")
			default:
				// noop
			}

			if _, found := processedIDs[v.ID]; found {
				fmt.Printf("%v: Ignore %v\n", in.Symbol, v)
				continue
			}

			_, err = stmt.ExecContext(
				ctx,
				v.ID,
				v.ExDate,
				v.Symbol,
				v.Amount,
				v.Currency,
				v.Frequency,
				v.PaymentType,
				time.Now(),
			)
			if err != nil {
				return fmt.Errorf("%v: %v", v, err)
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

		err = updateDividendAdj(ctx, runner, schema)
		if err != nil {
			return err
		}

		err = updateCloseAdj(ctx, runner, schema)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("%v: %v", in.Symbol, err)
	}
	return &divyield.DBSaveDividendsOutput{}, nil
}

func (db *DB) dividendIDs(
	ctx context.Context,
	runner runner,
	symbol string,
) (map[int64]struct{}, error) {
	ids := make(map[int64]struct{})
	schema := schemaStock(symbol)

	s, args, err := sq.Select("id").
		From(schema + ".dividend").
		PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := runner.QueryContext(ctx, s, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		ids[id] = struct{}{}
	}
	return ids, nil
}

func (db *DB) DividendYields(
	ctx context.Context,
	ticker string,
	f *divyield.DividendYieldFilter,
) ([]*divyield.DividendYield, error) {
	yields := make([]*divyield.DividendYield, 0)

	err := execNonTx(ctx, db.DB, func(runner runner) error {
		schema := schemaStock(ticker)

		q := sq.Select(
			"date",
			"close",
			"close_adj_splits",
			`coalesce(
                (select amount_adj from `+
				schema+`.dividend_view 
                where 
                    ex_date <= date and 
                    payment_type in ('Cash', 'Cash&Stock') and 
                    frequency > 0 
                order by ex_date desc limit 1), 0) 
                as div_amount_adj`,
			`coalesce(
                (select frequency from `+
				schema+`.dividend_view 
                where 
                    ex_date <= date and 
                    payment_type in ('Cash', 'Cash&Stock') and 
                    frequency > 0 
                order by ex_date desc limit 1), 0) 
                as div_freq`,
			`coalesce(
                (select sum(amount_adj) from `+
				schema+`.dividend_view 
                where 
                    ex_date >= (
                        date_trunc('month', CURRENT_DATE) 
                        - INTERVAL '12 months'
                    )::date and 
                    ex_date <= date_trunc(
                        'month', CURRENT_DATE
                    )::date and 
                    payment_type in ('Cash', 'Cash&Stock') and 
                    frequency > 0), 0) 
                as div_trail_ttm`,
		).
			From(schema + ".price").
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
			var close float64
			var closeAdjSplits float64
			var divAdj float64
			var frequency int
			var divTrailTTM float64

			err = rows.Scan(
				&date,
				&close,
				&closeAdjSplits,
				&divAdj,
				&frequency,
				&divTrailTTM,
			)
			if err != nil {

				fmt.Println("err: ", date.Format(divyield.DateFormat))
				return err
			}
			v := &divyield.DividendYield{
				Date:                   date,
				Close:                  close,
				CloseAdjSplits:         closeAdjSplits,
				DividendAdj:            divAdj,
				Frequency:              frequency,
				DividendAdjTrailingTTM: divTrailTTM,
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

func (db *DB) SaveSplits(
	ctx context.Context,
	in *divyield.DBSaveSplitsInput,
) (*divyield.DBSaveSplitsOutput, error) {
	err := execTx(ctx, db.DB, func(runner runner) error {
		schema := schemaStock(in.Symbol)

		if in.Reset {
			sql, args, err := sq.
				Delete("").
				From(schema + ".split").
				ToSql()

			_, err = runner.ExecContext(ctx, sql, args...)
			if err != nil {
				return err
			}
		}

		stmt, err := runner.PrepareContext(
			ctx,
			pq.CopyInSchema(
				schema,
				"split",
				"ex_date",
				"to_factor",
				"from_factor",
				"created",
			),
		)
		if err != nil {
			return err
		}

		for _, v := range in.Splits {
			select {
			case <-ctx.Done():
				return fmt.Errorf("interrupted")
			default:
				// noop
			}

			_, err = stmt.ExecContext(
				ctx,
				v.ExDate,
				v.ToFactor,
				v.FromFactor,
				time.Now(),
			)
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

		err = updateDividendAdj(ctx, runner, schema)
		if err != nil {
			fmt.Println("ERROR", err)
			return err
		}

		err = updateCloseAdj(ctx, runner, schema)
		if err != nil {
			fmt.Println("ERROR", err)
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return &divyield.DBSaveSplitsOutput{}, nil
}

func (db *DB) Splits(
	ctx context.Context,
	ticker string,
	f *divyield.SplitFilter,
) ([]*divyield.Split, error) {
	splits := make([]*divyield.Split, 0)

	err := execNonTx(ctx, db.DB, func(runner runner) error {
		schema := schemaStock(ticker)

		q := sq.Select(
			"ex_date",
			"to_factor",
			"from_factor",
		).
			From(schema + ".split").
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
			var toFactor float64
			var fromFactor float64

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

func (db *DB) SaveProfile(
	ctx context.Context,
	in *divyield.DBSaveProfileInput,
) (*divyield.DBSaveProfileOutput, error) {
	now := time.Now()

	err := execTx(ctx, db.DB, func(runner runner) error {

		s, args, err := sq.
			Select("true").
			From("public.profile").
			Where("symbol = ?", in.Symbol).
			PlaceholderFormat(sq.Dollar).
			ToSql()

		var exists bool
		err = runner.QueryRowContext(
			ctx,
			s,
			args...,
		).Scan(&exists)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		valuesMap := sq.Eq{
			"symbol":           in.Profile.Symbol,
			"name":             in.Profile.Name,
			"exchange":         in.Profile.Exchange,
			"issue_type":       in.Profile.IssueType,
			"industry":         in.Profile.Industry,
			"sector":           in.Profile.Sector,
			"description":      in.Profile.Description,
			"website":          in.Profile.Website,
			"primary_sic_code": in.Profile.PrimarySicCode,
			"address":          in.Profile.Address,
			"city":             in.Profile.City,
			"zip":              in.Profile.Zip,
			"state":            in.Profile.State,
			"country":          in.Profile.Country,
			"phone":            in.Profile.Phone,
			"pulled":           in.Profile.Pulled,
		}

		if exists {
			valuesMap["updated"] = now
			s, args, err = sq.
				Update("").
				Table("public.profile").
				SetMap(valuesMap).
				Where("symbol = ?", in.Symbol).
				PlaceholderFormat(sq.Dollar).
				ToSql()
		} else {
			valuesMap["created"] = now
			valuesMap["updated"] = now
			s, args, err = sq.
				Insert("").
				Into("public.profile").
				Columns(
					"symbol",
					"name",
					"exchange",
					"issue_type",
					"industry",
					"sector",
					"description",
					"website",
					"primary_sic_code",
					"address",
					"city",
					"zip",
					"state",
					"country",
					"phone",
					"created",
					"updated",
					"pulled",
				).
				SetMap(valuesMap).
				PlaceholderFormat(sq.Dollar).
				ToSql()
		}
		_, err = runner.ExecContext(ctx, s, args...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &divyield.DBSaveProfileOutput{}, nil
}

func (db *DB) Profiles(
	ctx context.Context,
	in *divyield.DBProfilesInput,
) (*divyield.DBProfilesOutput, error) {
	profiles := make([]*divyield.Profile, 0)

	err := execNonTx(ctx, db.DB, func(runner runner) error {
		q := sq.Select(
			"symbol",
			"name",
			"exchange",
			"issue_type",
			"industry",
			"sector",
			"description",
			"website",
			"primary_sic_code",
			"address",
			"city",
			"zip",
			"state",
			"country",
			"phone",
			"pulled",
		).
			From("public.profile").
			OrderBy("symbol asc").
			PlaceholderFormat(sq.Dollar)

		if len(in.Symbols) > 0 {
			q = q.Where(sq.Eq{"symbol": in.Symbols})
		}

		s, args, err := q.ToSql()
		if err != nil {
			return err
		}

		rows, err := runner.QueryContext(ctx, s, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var symbol string
			var name string
			var exchange string
			var issueType string
			var industry string
			var sector string
			var description string
			var website string
			var primarySicCode int
			var address string
			var city string
			var zip string
			var state string
			var country string
			var phone string
			var pulled *time.Time

			err = rows.Scan(
				&symbol,
				&name,
				&exchange,
				&issueType,
				&industry,
				&sector,
				&description,
				&website,
				&primarySicCode,
				&address,
				&city,
				&zip,
				&state,
				&country,
				&phone,
				&pulled,
			)
			if err != nil {
				return err
			}
			v := &divyield.Profile{
				Symbol:         symbol,
				Name:           name,
				Exchange:       exchange,
				IssueType:      issueType,
				Industry:       industry,
				Sector:         sector,
				Description:    description,
				Website:        website,
				PrimarySicCode: primarySicCode,
				Address:        address,
				City:           city,
				Zip:            zip,
				State:          state,
				Country:        country,
				Phone:          phone,
			}
			if pulled != nil {
				v.Pulled = *pulled
			}
			profiles = append(profiles, v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &divyield.DBProfilesOutput{
		Profiles: profiles,
	}, nil
}

func updateDividendAdj(
	ctx context.Context,
	runner runner,
	schema string,
) error {
	_, err := runner.ExecContext(
		ctx,
		"call public.update_dividend_adj($1);",
		schema,
	)
	return err
}

func updateCloseAdj(
	ctx context.Context,
	runner runner,
	schema string,
) error {
	_, err := runner.ExecContext(
		ctx,
		"call public.update_price_adj($1);",
		schema,
	)
	return err
}

type runner interface {
	ExecContext(
		context.Context,
		string,
		...interface{},
	) (sql.Result, error)

	PrepareContext(
		context.Context,
		string,
	) (*sql.Stmt, error)

	QueryContext(
		context.Context,
		string,
		...interface{},
	) (*sql.Rows, error)

	QueryRowContext(
		context.Context,
		string,
		...interface{},
	) *sql.Row
}

func execTx(
	ctx context.Context,
	db *sql.DB,
	fn func(runner runner) error,
) error {
	tx, err := db.BeginTx(
		ctx,
		&sql.TxOptions{
			Isolation: sql.LevelSerializable,
		},
	)
	if err != nil {
		return err
	}

	err = fn(tx)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf(
				"tx err: %v, rb err: %v",
				err,
				rbErr,
			)
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
