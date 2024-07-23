import sys
from log.monad_bft import BftLog

if __name__ == "__main__":
    log = BftLog.from_json(sys.stdin)
    print("Log Dataframe")
    print(log.df)
    ledger_commit_df = log.ledger_commit_df()
    print("\nLedger Commit Dataframe")
    print(ledger_commit_df)
    tx_per_second = ledger_commit_df["num_tx"].resample("s").sum()
    print("\nTransactions Per Second")
    print(tx_per_second)
    blocks_per_second = ledger_commit_df.resample("s").size()
    print("\nBlocks Per Second")
    print(blocks_per_second)
