import sys
from log.monad_bft import BftLog

if __name__ == "__main__":
    log = BftLog.from_json(sys.stdin)

    # TODO: interactive cli to allow users to choose what data to look at

    log.plot_block_commit()
    log.plot_received_proposal()
    log.plot_received_votes()
    log.missing_proposal_or_vote_df()
    log.plot_create_proposal()
    log.plot_timeout()
