"""
Script to produce a JSON file output that represents a link graph between the different drugs and their respective
mentions in the different PubMed publications, the different scientific publications and finally the journals with the
date associated with each of these mentions.
"""
import pandas as pd


# Function to find substring in string from different dataframe
def match_dataframe(df, df2, columns_to_match):
    """ This function makes it possible to find substrings of a column of a dataframe df in another string of a column
    of the dataframe df2. And the result is then returned
    :param df: dataframe of the substring to find
    :param df2: dataframe to search with the string to find
    :param columns_to_match: columns of strings to find. The first value corresponds to the column of the dataframe df
    and the other 2 are the columns of the dataframe df2
    :return:
    """
    df['join'] = 1
    df2['join'] = 1
    df_merged = df.merge(df2, on='join').drop('join', axis=1)
    df.drop('join', axis=1, inplace=True)
    # match string
    df_match = df_merged[df_merged.apply(
        lambda x: str(x[columns_to_match[1]]).upper().find(str(x[columns_to_match[0]]).upper()), axis=1).ge(
        0) | df_merged.apply(
        lambda x: str(x[columns_to_match[2]]).upper().find(str(x[columns_to_match[0]]).upper()), axis=1).ge(0)]
    return df_match


# function to call to produce the results of the json file of the links between
# the drugs and the publications
def drugs_result():
    # import files into dataframe
    df_drugs = pd.read_csv('files/drugs.csv')
    df_pubmed = pd.read_csv('files/pubmed.csv')
    df_clinical_trials = pd.read_csv('files/clinical_trials.csv')
    cols_to_match = ['drug', 'title', 'journal']
    # Add column for distinct publication
    df_pubmed['type_publication'] = 'pubmed'
    df_clinical_trials['type_publication'] = 'clinical_trials'
    df_clinical_trials.rename({'scientific_title': 'title'}, axis=1, inplace=True)
    # Concat dataframe
    frames = [df_pubmed, df_clinical_trials]
    df_concat_pubmed_clinical_trials = pd.concat(frames)
    # match string
    df_match = match_dataframe(df_drugs, df_concat_pubmed_clinical_trials, cols_to_match)
    # Build and Write result in JSON
    df_to_json = (df_match.groupby(['atccode', 'drug'])
                  .apply(lambda x: x[['id', 'title', 'date', 'journal', 'type_publication']].to_dict('records'))
                  .reset_index()
                  .rename(columns={0: 'publication'})
                  .to_json('files/match_drugs.json', orient='records'))


#
if __name__ == '__main__':
    drugs_result()
