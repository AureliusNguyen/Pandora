package ledger

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Querier is the read surface CheckInvariants needs; *pgxpool.Pool, *pgx.Conn
// and pgx.Tx satisfy it.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Violation is one failed invariant: Check names the query, Detail the
// offending txn, market or user id.
type Violation struct {
	Check  string
	Detail string
}

// Each query returns one text column naming the offending row; empty means OK.
var invariantQueries = []struct{ name, sql string }{
	{"txn_balanced", `SELECT txn_id::text FROM ledger_entries GROUP BY txn_id HAVING SUM(amount_cents) <> 0`},
	{"settled_escrow_zero", `SELECT m.id FROM markets m
		LEFT JOIN ledger_entries e ON e.account_id = 'acct_escrow_' || m.id
		WHERE m.status IN ('resolved','voided') GROUP BY m.id HAVING COALESCE(SUM(e.amount_cents),0) <> 0`},
	{"wallet_matches_entries", `SELECT w.user_id FROM wallets w
		LEFT JOIN ledger_entries e ON e.account_id = 'acct_cash_' || w.user_id
		GROUP BY w.user_id, w.balance_cents HAVING w.balance_cents <> COALESCE(SUM(e.amount_cents),0)`},
	{"complete_set_symmetry", `SELECT market_id FROM (
		SELECT o.market_id, o.idx, COALESCE(SUM(p.qty),0) AS s FROM outcomes o
		JOIN markets m ON m.id = o.market_id
		LEFT JOIN positions p ON p.market_id = o.market_id AND p.outcome = o.idx AND p.settled_at IS NULL
		WHERE m.status NOT IN ('resolved','voided') GROUP BY o.market_id, o.idx) t
		GROUP BY market_id HAVING MIN(s) <> MAX(s)`},
}

// CheckInvariants runs the four ledger invariant queries from the contracts
// and returns every violation; an empty slice means the ledger is consistent.
// backend/deploy/invariants.sql must stay a verbatim copy of these queries.
func CheckInvariants(ctx context.Context, q Querier) ([]Violation, error) {
	var out []Violation
	for _, iq := range invariantQueries {
		rows, err := q.Query(ctx, iq.sql)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", iq.name, err)
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", iq.name, err)
		}
		for _, id := range ids {
			out = append(out, Violation{Check: iq.name, Detail: id})
		}
	}
	return out, nil
}
