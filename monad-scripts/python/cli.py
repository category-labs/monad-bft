import sys
from log.monad_bft import BftLog

if __name__ == "__main__":
    log = BftLog.from_json(sys.stdin)
    sys.stdin.close()
    sys.stdin = open('/dev/tty')

    # interactive cli to allow users to choose what data to look at
    print("Select the data you would like to look at")
    print("1. summary of common metrics")
    print("2. duration between block commits")
    print("3. duration between receiving proposals")
    print("4. duration to collect votes when proposing")
    print("5. duration to collect transactions when proposing")
    print("6. duration between timeout to next proposal")
    print("7. round numbers where node does not receive a proposal or create a vote")
    print("8. all of the above")

    user_input = input("Select a number from 1-8: ")
    if user_input == "1":
        log.summary()
    elif user_input == "2":
        log.plot_block_commit()
    elif user_input == "3":
        log.plot_received_proposal()
    elif user_input == "4":
        log.plot_received_votes()
    elif user_input == "5":
        log.plot_create_proposal()
    elif user_input == "6":
        log.plot_timeout()
    elif user_input == "7":
        log.missing_proposal_or_vote_df()
    elif user_input == "8":
        log.summary()
        log.plot_block_commit()
        log.plot_received_proposal()
        log.plot_received_votes()
        log.plot_create_proposal()
        log.plot_timeout()
        log.missing_proposal_or_vote_df()
    else:
        print("Invalid input")
