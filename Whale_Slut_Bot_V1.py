# --------  WHALE SLUT ROBOT   ---------------------

#Import Libraries
from solana.rpc.api import Client
import solana
import solders
from solders.pubkey import Pubkey
import pandas as pd
import time
from print_color import print
import pusher
import os
import dotenv

client = Client("https://api.mainnet-beta.solana.com")

###Token's List 

known_tokens_price_list = [
    {"name": "Wrapped SOL", "Mint Token Address": "So11111111111111111111111111111111111111112","price":150 },
    {"name": "Jupiter", "Mint Token Address": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN","price":0.76},
    {"name": "USD Coin", "Mint Token Address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","price":1},
    {"name": "Pyth Network", "Mint Token Address": "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3","price":0.28},
    {"name": "JITO", "Mint Token Address": "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL" ,"price":2.33},
    {"name": "Wormhole Token", "Mint Token Address": "85VBFQZC9TZkfaptBWjvUw7YbZjy52A6mjtPGjstQAmQ" ,"price":0.22 },
    {"name": "Bonk", "Mint Token Address": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263" ,"price":0.000017},
    {"name": "USDT", "Mint Token Address": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB","price":1},
    {"name": "Jito Staked SOL", "Mint Token Address": "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn","price":160},
    {"name": "Dog With Hat", "Mint Token Address": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm","price":1.4}
]

#FUNCTIONS

def check_tx(signature):
    """Insert Transaction signature as string to return transaction details"""
    checking_tx = solders.signature.Signature.from_string(signature)
    tx = client.get_transaction(checking_tx)
    print("Transaction Info: ", tag="Success", background="green")
    return tx

###
def get_block_info(slot_number):
    """Getting Block Data of a specific slot 
    slot_number must be given in integer
    """   
    block_info = client.get_block(slot_number, max_supported_transaction_version=1).value
    return block_info

###
def live_slot_number():
    """live_slot_number calls the integer value of final slot number that was executed"""
    live_slot = client.get_slot().value
    print("{}".format(live_slot), color="black", tag=("SLOT NUMBER"), background="yellow")
    return live_slot

###
def transactions(block_info):
    """Get each transaction signature and wallet address information from the block itself, return Pandas DataFrame"""
    length_of_transactions = len(block_info.transactions)
    print("{}".format(length_of_transactions), color="black", tag=("TRANSACTION COUNT"), background="cyan")
    transactions_list = []
    wallets_list = []
    empty_value_transactions_count = []
    for i in range(length_of_transactions):
        if block_info.transactions[i].meta == None:
            empty_value_transactions_count.append("")
        else: 
            txSignature = block_info.transactions[i].transaction.signatures
            wallet_account_address = block_info.transactions[i].transaction.message.account_keys[0] #0 is the constant signer which is the signer
            transactions_list.append(txSignature)
            wallets_list.append(wallet_account_address)
            
        dict = {'Wallets': wallets_list,
                             "Signatures" : transactions_list} 
        df = pd.DataFrame(dict) 
    print("{}".format(len(empty_value_transactions_count)), color="black", tag=("NONE METADATA TRANSACTION COUNT"), background="magenta")
    return df

###
def wallet_token_balances(block_info):
    """"Get each transaction Pre and Post Token Balances as oblects from the block itself, return Pandas DataFrame
    List legnths are same with
    transactions()"""
    pre_tokens_list = []
    post_tokens_list = []
    valueless_tx_list = []
    length_of_transactions = len(block_info.transactions)
    for j in range(length_of_transactions):
        pre_tokens = block_info.transactions[j].meta.pre_token_balances
        post_tokens = block_info.transactions[j].meta.post_token_balances
        pre_tokens_list.append(pre_tokens)
        post_tokens_list. append(post_tokens)

        #Additionally Count Valueless transactions
        if len(pre_tokens) == 0 & len(post_tokens) == 0:
            valueless_tx_list.append("")
        else:
            pass
    
    #Cook DFs
    dict = {'Pre_Token_Balances': pre_tokens_list,
            "Post_Token_Balances" : post_tokens_list} 
    df = pd.DataFrame(dict) 
    #Print DF Informations of transactions
    print("{}".format(len(valueless_tx_list)), color="white", tag=("VALUELESS TRANSACTION COUNT"), tag_color="white", background="gray")
    print("{}".format(len(pre_tokens_list)-len(valueless_tx_list)), color="white", tag=("VALUEABLE TRANSACTION COUNT"), background="gray")
    print("{}".format(len(pre_tokens_list)), color="white", tag=("TOTAL TRANSACTION COUNT"), tag_color="white", background="blue")        
    return df


###
def pre_post_value_filter(df):
    """Getting Pre and Post Token balances of transactions -> seperating and processing according to token account list
    sizes due to unevennes of the lists.
    
    returns pandas DataFrame as seperated as follows
    PRE = POST = 0 : zero_value_list : empty array of 0's (just used for count)
    PRE = POST > 0 : post_eql_pre_list
    PRE > POST : pre_big_post_list
    PRE < POST : post_big_pre_list
    """

    zero_value_list = pd.DataFrame()
    pre_big_post_list = pd.DataFrame()
    post_big_pre_list = pd.DataFrame()
    post_eql_pre_list = pd.DataFrame()

    for k in range(len(df)):
        pre_token_counts = df["Pre_Token_Balances"][k]
        post_token_counts = df["Post_Token_Balances"][k]
        if pre_token_counts == [] and post_token_counts == []:
            zero_value_list = pd.concat([zero_value_list, pd.DataFrame([""])], axis=0)
        elif len(pre_token_counts) > len(post_token_counts):
            pre_big_post_list = pd.concat([pre_big_post_list, df.iloc[[k]]], axis=0, ignore_index=True)
        elif len(pre_token_counts) < len(post_token_counts):
            post_big_pre_list = pd.concat([post_big_pre_list, df.iloc[[k]]], axis=0, ignore_index=True)
        elif len(pre_token_counts) > 0 and len(post_token_counts)>0 : #Post=Pre
            post_eql_pre_list = pd.concat([post_eql_pre_list, df.iloc[[k]]], axis=0, ignore_index=True)  
        else: 
            raise Exception("There is a problem with per-post number match")
            
    print("{}".format(k), color="black", tag=("TX COUNT"), tag_color="black", background="yellow")        
    print("{}".format(len(zero_value_list)), color="black", tag=("ZERO VALUE COUNT"), tag_color="red", background="yellow")        
    print("{}".format(len(pre_big_post_list)), color="blue", tag=("PRE > POST COUNT"), tag_color="white", background="red")
    print("{}".format(len(post_big_pre_list)), color="yellow", tag=("PRE < POST COUNT"), tag_color="white", background="red")
    print("{}".format(len(post_eql_pre_list)), color="black", tag=("PRE = POST COUNT"), tag_color="black", background="green")
    return (zero_value_list, pre_big_post_list, post_big_pre_list, post_eql_pre_list)
    

###
def eql_change_token_list(post_eql_pre_list):
    """Insert: PRE = POST > 0 : post_eql_pre_list
    returns : Data Frame with discarded wallet addresses and change amounts from solders objects"""
    df1 = pd.DataFrame()
    for i in range(len(post_eql_pre_list)):
        for j in range(len(post_eql_pre_list["Pre_Token_Balances"][i])):
            pre_mint_token_address = post_eql_pre_list["Pre_Token_Balances"][i][j].mint
            pre_token_ui_amount = post_eql_pre_list["Pre_Token_Balances"][i][j].ui_token_amount.ui_amount
            
            post_mint_token_address = post_eql_pre_list["Post_Token_Balances"][i][j].mint
            post_token_ui_amount = post_eql_pre_list["Post_Token_Balances"][i][j].ui_token_amount.ui_amount
            
            if pre_token_ui_amount==None or post_token_ui_amount==None:
                change_amount=0
            else:
                change_amount = post_token_ui_amount - pre_token_ui_amount
            
            #print("TX No: {}".format(i), background="blue")
            #print("Pre Address:", pre_mint_token_address, " - ", "Pre Amount", pre_token_ui_amount)
            #print("Post Address:", post_mint_token_address, " - ", "Post Amount", post_token_ui_amount)
            if pre_mint_token_address == post_mint_token_address:
                data = {"Wallets" : post_eql_pre_list["Wallets"][i],
                        "Signatures" : post_eql_pre_list["Signatures"][i],
                        "Mint Token Address" : str(pre_mint_token_address),
                        "Token Change Amount" : change_amount
                       }
                df = pd.DataFrame(data)
                df1 = pd.concat([df1,df])
                #print("Trasnacting_Token_Address:", pre_mint_token_address, " - ", "Change_Amount", change_amount)
            else:
                raise Exception("There is a problem with per-post address match")
    return df1

####
def whale_slut_bot(slot_number):
    """insert: Slot number to initiate
    returns: whale_df : pd.DataFrame()"""

    print("{}".format(slot_number), color="black", tag=("SLOT NUMBER"), background="yellow")
    block_info = get_block_info(slot_number)
    
    #Get transaction details of the block
    tx_df = transactions(block_info)
    
    #Get Wallet Address and Tx Signiture
    tkn_df = wallet_token_balances(block_info)
    
    #Concat dataframes
    block_df = pd.concat([tx_df, tkn_df], axis=1)
    
    #Get seperate the transaction Token Balances Pre vs Post and appoint dataframes in orderly
    post_pre_status_df = pre_post_value_filter(block_df)
    pre_big_post_list = post_pre_status_df[1]
    post_big_pre_list = post_pre_status_df[2]
    post_eql_pre_list = post_pre_status_df[3]
    
    #Rearranging objects to list of PRE=POST>0 : OTHERS WILL BE ADDED
    df_tx_change_with_wallets = eql_change_token_list(post_eql_pre_list)
    
    #Get Price Information : There should be the live one + There are unknow tokens and their prices.
    #There will beAMM Router Price Returner function
    df_known_tokens_price = pd.DataFrame(known_tokens_price_list)
    
    #Merging Price information according to Mint Token Address 
    df3 = pd.merge(df_tx_change_with_wallets,df_known_tokens_price, on="Mint Token Address",how="left")
    
    #Making new column price*token amount  
    df3["Tx_Value"] = df3["Token Change Amount"]*df3["price"]
    
    #Insert Whale Defining Limit
    whale_tx_limit = 9_000 #USD
    whale_df = df3[abs(df3['Tx_Value']) >= whale_tx_limit]
    
    #Return Whale List
    print(whale_df)
    return whale_df

###Give Starting Slot to infinite loop

slot_number = 284917557 #starting slot to be scanned
market_ID = "whales" #websocket channel constant


#Infinite Loop
while True:
    slot_number += 1
    df_whales = pd.concat([df_whales, whale_slut_bot(slot_number)],ignore_index=True)
    change = pd.DataFrame.to_json(df_whales) #Transform data to Json from dataframe

    #Push Data to websocket
    pusher_client = pusher.Pusher(
        app_id=os.getenv('SOKETI_APP_ID'),
        key=os.getenv('SOKETI_APP_KEY'),
        secret=os.getenv('SOKETI_APP_SECRET'),
        cluster="mt1",
        host=os.getenv('SOKETI_HOST'),
        port=int(os.getenv('SOKETI_PORT')),
        ssl=os.getenv('SOKETI_USE_TLS') == 'true',
        scheme=os.getenv('SOKETI_SCHEME')
    )
    

    try:
        response = pusher_client.trigger(marketID, "update", change)
        if response['status'] != 200:
            raise Exception("unexpected status")
        # Optionally, process the response further
        # response_json = response['body']
    except Exception as error:
        # Handle error
        # logger.info(f"yo mumma {error}")
        # errCount += 1
        print(f"Error: {error}")
    
    
    
