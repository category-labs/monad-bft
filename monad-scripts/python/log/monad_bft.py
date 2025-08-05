import pandas as pd
import matplotlib.pyplot as plt

class BftLog:
    def __init__(self, df):
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['message'] = df['fields'].apply(lambda x: x.pop('message'))
        self.df = df


    ##############################################################################
    #                     DATA PARSING HELPER FUNCTIONS                          #
    ##############################################################################

    # collect duration between committed blocks
    def block_commit_df(self):
        df = self.df[
            (self.df['target'] == 'monad_ledger')
            & (self.df['message'] == 'committed block')
        ]
        df = df[['timestamp', 'fields']]
        df['num_tx'] = df['fields'].apply(lambda x: x.get('num_tx'))
        df['block_num'] = df['fields'].apply(lambda x: x.get('block_num'))
        df = df.drop('fields', axis=1)

        # calculate duration between each committed block (in milliseconds)
        df['duration'] = df['timestamp'].diff().dt.total_seconds() * 1000
        df = df.dropna(subset=['duration'])

        return df


    # collect duration between receiving consecutive proposals
    def received_proposal_df(self):
        df = self.df[
            (self.df['target'] == 'monad_consensus_state')
            & (self.df['message'] == 'Received Proposal')
        ]
        df = df[['timestamp', 'fields']]
        df['round'] = df['fields'].apply(lambda x: x.get('round')).astype(int)
        df = df.drop('fields', axis=1)

        # calculate duration between receiving proposals (in milliseconds)
        df['duration'] = df['timestamp'].diff().dt.total_seconds() * 1000
        df = df.dropna(subset=['duration'])

        return df


    # collect data when the node is the block leader and collect votes from its peers
    # shows the duration to collect the votes and the total number of votes collected
    def received_votes_df(self):
        df = self.df[
            (self.df['target'] == 'monad_consensus::vote_state')
            & (self.df['message'] == 'collecting vote')
        ]
        df = df[['timestamp', 'target', 'fields']]
        df['round'] = df['fields'].apply(lambda x: x.get('round')).astype(int)
        df = df.drop('fields', axis=1)

        # group by round and aggregate
        df_grouped = df.groupby('round').agg({
            'timestamp': ['min', 'max', 'count']
        }).reset_index()
        df_grouped.columns = ['round', 'min_timestamp', 'max_timestamp', 'total_votes']

        # calculate duration to obtain total votes
        df_grouped['timestamp_diff'] = df_grouped['max_timestamp'] - df_grouped['min_timestamp']

        # only keep selected columns
        df = df_grouped[['total_votes', 'round', 'timestamp_diff']]

        # convert duration to milliseconds
        df = df.copy()
        df['duration'] = df['timestamp_diff'].dt.total_seconds() * 1000
        df['duration'] = df['duration'].round(2)
        df = df.drop('timestamp_diff', axis=1)

        return df


    # collect data when the node is the block leader and create a proposal
    # shows the duration to collect transaction and create proposal
    def create_proposal_df(self):
        df = self.df[
            (
                (self.df['target'] == 'monad_consensus_state') &
                (self.df['message'] == 'Creating Proposal')
            ) | (
                (self.df['target'].str.startswith('monad_eth_txpool')) &
                (self.df['message'] == 'created proposal')
            )
        ]
        df = df[['timestamp', 'fields', 'target']]
        df['seq_num'] = df.apply(lambda row: row['fields']['try_propose_seq_num'] if row['target'] == 'monad_consensus_state' else row['fields']['proposed_seq_num'], axis=1).astype(int)
        df['num_tx'] = df.apply(lambda row: 0 if row['target'] == 'monad_consensus_state' else row['fields']['proposal_num_tx'], axis=1).astype(int)
        df = df.drop(['fields', 'target'], axis=1)

        # group by round and aggregate
        df_grouped = df.groupby('seq_num').agg({
            'num_tx': ['max'],
            'timestamp': ['min', 'max'],
        }).reset_index()
        df_grouped.columns = ['seq_num', 'num_tx', 'min_timestamp', 'max_timestamp']

        # calculate duration to create proposal
        df_grouped['timestamp_diff'] = df_grouped['max_timestamp'] - df_grouped['min_timestamp']

        # only keep selected columns
        df = df_grouped[['seq_num', 'num_tx', 'timestamp_diff']]

        # convert duration to milliseconds
        df = df.copy()
        df['duration'] = df['timestamp_diff'].dt.total_seconds() * 1000
        df['duration'] = df['duration'].round(2)
        df = df.drop('timestamp_diff', axis=1)

        return df


    # flag the round numbers where the node does not receive a proposal or create a vote
    def missing_proposal_or_vote_df(self):
        df_proposal = self.df[
            (self.df['target'] == 'monad_consensus_state') &
            (self.df['message'] == 'Received Proposal')
        ]
        df_vote = self.df[
            (self.df['target'] == 'monad_consensus_state') &
            (self.df['message'] == 'created vote')
        ]
        rounds_proposal = df_proposal['fields'].apply(lambda x: x.get('round')).astype(int)
        rounds_vote = df_vote['fields'].apply(lambda x: x.get('round')).astype(int)

        # identity missing rounds for proposal
        full_range = set(range(rounds_proposal.min(), rounds_proposal.max() + 1))
        missing_rounds = sorted(full_range - set(rounds_proposal))
        print("Missing proposal rounds:", missing_rounds)

        # identify missing rounds for voting
        full_range = set(range(rounds_vote.min(), rounds_vote.max() + 1))
        missing_rounds = sorted(full_range - set(rounds_vote))
        print("Missing voting rounds:", missing_rounds)


    # collect duration between a local timeout to receiving next proposal
    def timeout_df(self):
        df = self.df[
            (
                (self.df['target'] == 'monad_consensus_state') & 
                (self.df['message'] == 'local timeout')
            ) | (
                (self.df['target'] == 'monad_consensus_state') & 
                (self.df['message'] == 'Received Proposal')
            )
        ]
        df = df[['timestamp', 'message', 'fields']]
        df['round'] = df['fields'].apply(lambda x: x.get('round')).astype(int)
        df = df.drop('fields', axis=1).reset_index()

        last_timeout_index = None
        last_timeout_timestamp = None
        last_timeout_round = None
        results = []

        for index, row in df.iterrows():
            if row['message'] == 'local timeout':
                if last_timeout_index is None:
                    last_timeout_timestamp = df.loc[index, 'timestamp']
                    last_timeout_round = df.loc[index, 'round']
                # else consecutive timeout rounds so do not update last_timeout_timestamp
                last_timeout_index = index
            elif row['message'] == 'Received Proposal' and last_timeout_index is not None:
                # calculate the duration
                proposal_timestamp = row['timestamp']
                duration = (proposal_timestamp - last_timeout_timestamp).total_seconds() * 1000
                results.append({
                    'timeout_timestamp': last_timeout_timestamp,
                    'proposal_timestamp': proposal_timestamp,
                    'round': last_timeout_round,
                    'duration': duration
                })
                last_timeout_index = None
                last_timeout_timestamp = None

        result_df = pd.DataFrame(results)

        return result_df


    ##############################################################################
    #                     VISUALIZATION HELPER FUNCTIONS                         #
    ##############################################################################

    # plot the duration between committed blocks
    def plot_block_commit(self):
        df = self.block_commit_df()

        plt.figure(figsize=(12, 6))
        plt.scatter(df['block_num'], df['duration'], color='blue', s=5)
        plt.xlabel('Block Number')
        plt.ylabel('Duration (ms)')
        plt.title('Duration between consecutive blocks')
        plt.savefig('block_time.png')
        plt.close()


    # plot the graph of proposal duration between each round
    def plot_received_proposal(self):
        df = self.received_proposal_df()

        plt.figure(figsize=(12, 6))
        plt.scatter(df['round'], df['duration'], color='blue', s=5)
        plt.xlabel('Round')
        plt.ylabel('Duration (ms)')
        plt.title('Duration between consecutive proposals')
        plt.savefig('proposal_time.png')
        plt.close()


    # plot the duration to collect the votes and the total number of votes collected
    def plot_received_votes(self):
        df = self.received_votes_df()

        # create a figure and axis objects
        fig, ax1 = plt.subplots(figsize=(12, 6))

        # primary y-axis
        color = 'tab:blue'
        ax1.set_xlabel('Round')
        ax1.set_ylabel('Duration (ms)', color=color)
        ax1.plot(df['round'], df['duration'], color=color)
        ax1.tick_params(axis='y', labelcolor=color)

        # secondary y-axis
        ax2 = ax1.twinx()
        color = 'tab:orange'
        ax2.set_ylabel('Total Votes', color=color)
        ax2.plot(df['round'], df['total_votes'], color=color)
        ax2.tick_params(axis='y', labelcolor=color)

        plt.title('Duration taken to collect votes by leader')
        fig.tight_layout()
        plt.savefig('vote_collection.png')
        plt.close()


    # plot the duration to collect transaction and create proposal
    def plot_create_proposal(self):
        df = self.create_proposal_df()

        plt.figure(figsize=(12, 6))
        plt.plot(df['seq_num'], df['duration'], color='blue')
        plt.xlabel('Sequence number')
        plt.ylabel('Duration (ms)')
        plt.title('Duration for creating proposals')
        plt.savefig('proposal_creation.png')
        plt.close()


    # plot the graph of duration of timeouts to next proposal
    def plot_timeout(self):
        df = self.timeout_df()

        plt.figure(figsize=(12, 6))
        plt.scatter(df['round'], df['duration'], color='blue', s=50)
        plt.xlabel('Round')
        plt.ylabel('Duration (ms)')
        plt.title('Duration between timeouts and next proposal')
        plt.savefig('timeout_duration.png')
        plt.close()


    @staticmethod
    def from_json(filepath_or_buffer):
        df = pd.read_json(filepath_or_buffer, lines=True)
        return BftLog(df)
