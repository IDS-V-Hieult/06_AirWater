import json
import sys
import datetime

import boto3
import botocore
import os

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.functions import row_number
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

import random
import string

import redshift_connector

import psycopg2
# from psycopg2.extras import DictCursor
import re

import socket
import time

import pandas as pd
import numpy as np
import csv

import io 

import traceback

import pytz
import logging


BOTO3_CONFIG = botocore.config.Config(
   retries = {
      'max_attempts': 10,
      'mode': 'standard'
   }
)


class MainFlow():
    #S3の各種パス
##    ETL_DIR = "ETL"
##    UPLOAD_FILE_DIR = ETL_DIR+'/'+"upload"
##    BACKUP_FILE_DIR = ETL_DIR+'/'+"bk"
##    ERROR_FILE_DIR = ETL_DIR+'/'+"error"

    #DB接続のリトライ回数
    DWH_RETRY_MAX = 3
    #DB接続のリトライの待ち時間
    DWH_RETRY_WAIT = 3

    #DBに登録するログメッセージの上限
    DB_LOG_MAX_LENGTH = 5000

    #バルクインサートの最大行数
    MAX_BULK_INSERT = 5000

    #開発用ログ表示
    IS_DEBUG = True

    #開発確認用ファイル移動停止
    IS_DEBUG_NO_FILE_MOVE_OPERATION = False

    #完了ステータス
    JOB_STATUS = True
    

    DEBUG_TIME = ""

##    REGEX_SINGLE_CHAR = "^[ -~]*$"
##    REGEX_INT = "^-?[0-9]+$"
##    REGEX_DECIMAL = "^-?[0-9]+(\.[0-9]+)?$"
##    REGEX_BOOL = "^(TRUE|True|true|FALSE|False|false)$"
##    REGEX_DATE = "^(((19[0-9]{2}|2[0-9]{3})[/|\-]?(0?1|0?3|0?5|0?7|0?8|10|12)[/|\-]?(0?[1-9]|[12][0-9]|3[01]))|((19[0-9]{2}|2[0-9]{3})[/|\-]?(0?4|0?6|0?9|11)[/|\-]?(0[1-9]|[12][0-9]|30))|((19[0-9]{2}|2[0-9]{3})[/|\-]?02[/|\-]?(0?[1-9]|1[0-9]|2[0-8]))|((19|2[0-9])([02468][048]|[13579][26])[/|\-]?0?2[/|\-]?29))$"
##    REGEX_DATETIME = "^((((19[0-9]{2}|2[0-9]{3})[/|\-]?(0?1|0?3|0?5|0?7|0?8|10|12)[/|\-]?(0?[1-9]|[12][0-9]|3[01]))|((19[0-9]{2}|2[0-9]{3})[/|\-]?(0?4|0?6|0?9|11)[/|\-]?(0[1-9]|[12][0-9]|30))|((19[0-9]{2}|2[0-9]{3})[/|\-]?02[/|\-]?(0?[1-9]|1[0-9]|2[0-8]))|((19|2[0-9])([02468][048]|[13579][26])[/|\-]?0?2[/|\-]?29))([ |T]([01][0-9]|2[0-3]):?[0-5][0-9](:?[0-5][0-9](\.[0-9]{1,6})?)?(Z|(\\\+0[0-9]:?[0-5][0-9])|(\\\+1[0-4]:?[0-5][0-9])|(\\\-0[0-9]:?[0-5][0-9])|(\\\-1[0-2]:?[0-5][0-9]))?)?)$"
##    REGEX_TIME = "^([01][0-9]|2[0-3]):?[0-5][0-9]:?[0-5][0-9](\.[0-9]{1,3})?(Z|\\\+0[0-9]:?[0-5][0-9]|\\\+1[0-4]:?[0-5][0-9]|\\\-0[0-9]:?[0-5][0-9]|\\\-1[0-2]:?[0-5][0-9])?$"


    def __init__(self):
        self.args = getResolvedOptions(
            sys.argv,
            [
                'JOB_NAME',
                'SECRETS_NAME',
##                'DATA_LAKE_S3_BUCKET',
                'DWH_DB_HOST',
                'DWH_DB_PORT',
                'DWH_DB_NAME',
                'CUSTOMER_ID',
                'DWH_IAM_ROLE_ARN',
                'AURORA_HOST',
                'AURORA_PORT',
                'AURORA_SECRET',
                'AURORA_DB_NAME',
                'TARGET_TERM_HOUR',
                'ALARM_SNS'
            ]
        )

        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)

        #timezoneの設定
        self.tz = datetime.timezone(datetime.timedelta(hours=9))

        self.now = datetime.datetime.now(self.tz)
        self.epoch = int(self.now.timestamp())

        self.customer_id = self.args['CUSTOMER_ID']

        dt_now = datetime.datetime.now(self.tz)
        self.str_dt_now = dt_now.strftime('%Y/%m/%d %H:%M:%S')
        self.etl_execule_time = dt_now.strftime('%Y%m%d%H%M%S')
        self.process_id = dt_now.strftime('%Y%m%d%H%M%S')
        
        #ファイル毎Error通知メール送信フラグ
        self.is_sent_error_mail = False
        self.is_sent_fatal_mail = False

        #ファイルコード
        self.file_code = ''

        #DWHコネクション
        self.dwh_connection = None

        #Auroraコネクション
        self.Aurora_connection = None

        self.file_path = ''


        #各種テーブル
        self.target_work_schema = ''
        self.target_work_table = ''
        self.target_schema = ''
        self.target_table = ''

        self.time_term = {}

        #各種処理件数カウント
        self.total_num = 0
        self.success_num = 0
        self.error_num = 0

        #ログ出力カウント
        self.log_seq = 0

        #エラー発生フラグ
        self.is_error = False

        #IAMロールのARN
        self.dwh_iam_role_arn = self.args["DWH_IAM_ROLE_ARN"]

        self.spark_context = SparkSession.builder.getOrCreate()
        self.glue_context = GlueContext(self.spark_context)
        self.logger = self.glue_context.get_logger()

        self.job = Job(self.glue_context)
        self.job.init(self.args['JOB_NAME'], self.args)
        self.Logging('job init', 'info')

        self.SQL_NOW = 'GETDATE()'
        self.aws_session = boto3.Session()
        self.aws_secretsmanager = self.aws_session.client(
            'secretsmanager',
            config = BOTO3_CONFIG
        )
        self.aws_sns = self.aws_session.client(
            'sns',
            config = BOTO3_CONFIG
        )
#        self.aws_s3 = self.aws_session.client(
#            's3',
#            config = BOTO3_CONFIG
#        )
        
        self.dwh_db_secrets = self.GetSecretValue()
        self.Auorra_db_secrets = self.GetAuroraSecretValue()

        self.rsc_url = 'postgresql://{}:{}@{}:{}/{}'.format(
            self.args['DWH_DB_HOST'],
            self.args['DWH_DB_NAME'],
            self.args['DWH_DB_PORT'],
            self.dwh_db_secrets['username'],
            self.dwh_db_secrets['password']
        )
        # self.psycopg2_url = 'postgresql://{}:{}@{}:{}/{}'.format(
        #     self.dwh_db_secrets['user'],
        #     self.dwh_db_secrets['pass'],
        #     self.args['DWH_DB_HOST'],
        #     self.args['DWH_DB_PORT'],
        #     self.args['DWH_DB_NAME']
        # )
        self.jdbc_url = 'jdbc:{}://{}:{}/{}'.format(
            'redshift',
            self.args['DWH_DB_HOST'],
            self.args['DWH_DB_PORT'],
            self.args['DWH_DB_NAME']
        )


    def Logging(self, message, level):
        if level == 'info':
            self.logger.info(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'error':
            self.logger.error(message)
        elif level == 'critical':
            self.logger.critical(message)
        else:
            self.logger.error(message)

    def GetSecretValue(self):
        response = self.aws_secretsmanager.get_secret_value(
            SecretId = self.args['SECRETS_NAME'],
        )
        
        self.Logging('Success get_secret_value', 'info')
        return json.loads(response['SecretString'])
    
    def GetAuroraSecretValue(self):
        response = self.aws_secretsmanager.get_secret_value(
            SecretId = self.args['AURORA_SECRET']
        )
        
        self.Logging('Success get_aurora_secret_value', 'info')
        return json.loads(response['SecretString'])

    
    def JobCommit(self):
        self.job.commit()
        self.Logging('job commit', 'info')
    
    def TriggerPublish(self):
        #後続のデータセット更新用のSNS連携
        #DATA_SET_IDが廃止になったためIngestionIdのみ連携
        return True
        # _ = self.aws_sns.publish(
        #     TopicArn = self.args['AFTER_CALL_SNS'],
        #     Message = json.dumps(
        #         {
        #             'IngestionId': str(self.epoch)
        #         }
        #     )
        # )

        self.Logging('Success publish', 'info')

    def LoggingWithTime(self, msg, level):
        if self.DEBUG_TIME == "":
            self.DEBUG_TIME = datetime.datetime.now(self.tz)
        
        now = datetime.datetime.now(self.tz)
        diff = now-self.DEBUG_TIME
        self.DEBUG_TIME = now

        self.Logging(msg+' ('+str(diff.total_seconds())+'sec)', 'info')
    
    def MeasureTime(self, title, status):
        now = datetime.datetime.now(self.tz)
        if status == 'start':
            self.time_term[title+'_start'] = now
        elif status == 'end':
            diff = now - self.time_term.get(title+'_start')
            if title+'_total' in self.time_term:
                self.time_term[title+'_total'] = self.time_term.get(title+'_total')+diff
            else:
                self.time_term[title+'_total'] = diff
        elif status == 'time':
            return self.time_term.get(title+'_total')
        
        return True



    
    ##########################################################
    #処理開始メール送信
    ##########################################################
    def SendJobStartMail(self):
        # _ = self.aws_sns.publish(
        #     TopicArn = self.args['ALARM_SNS'],
        #     Subject = '【Info】ETL処理開始【'+self.customer_id+'】',
        #     Message = 'ETL処理開始\n'\
        #             + '顧客ID : '+self.customer_id+'\n'\
        #             + '処理ID : '+self.process_id+'\n'
        # )
        return True

    ##########################################################
    #処理終了メール送信
    #status
    # 正常終了:True　異常終了:False
    ##########################################################
    def SendJobFinishMail(self, status=True):
        #正常終了
        if status:
            _ = self.aws_sns.publish(
                TopicArn = self.args['ALARM_SNS'],
                Subject = '【Info】ETL処理正常終了【'+self.customer_id+'】',
                Message = 'ETL処理正常終了\n'\
                        + '顧客ID : '+self.customer_id+'\n'\
                        + '処理ID : '+self.process_id+'\n'
            )
        #異常終了
        else:
            _ = self.aws_sns.publish(
                TopicArn = self.args['ALARM_SNS'],
                Subject = '【Info】ETL処理異常終了【'+self.customer_id+'】',
                Message = 'ETL処理異常終了\n'\
                        + '顧客ID : '+self.customer_id+'\n'\
                        + '処理ID : '+self.process_id+'\n'
            )
        # return True

    ##########################################################
    #エラーメール送信
    ##########################################################
    def SendErrorMail(self, file_name=''):
        _ = self.aws_sns.publish(
            TopicArn = self.args['ALARM_SNS'],
            Subject = '【Error】ETL処理中にエラーが発生【'+self.customer_id+'】',
            Message = '['+file_name+']を処理中にエラーが発生しました。\n'\
                    + '顧客ID : '+self.customer_id+'\n'\
                    + '処理ID : '+self.process_id+'\n'
        )
    
    ##########################################################
    #Fatalメール送信
    ##########################################################
    def SendFatalMail(self, file_name=''):
        msg = ''
        if file_name is not None:
            msg = '['+file_name+']を処理中にFatalエラーが発生しました。\n'
        else:
            msg = 'ETL処理中にFatalエラーが発生しました。\n'
        
        msg = msg+ '顧客ID : '+self.customer_id+'\n'\
            + '処理ID : '+self.process_id+'\n'

        _ = self.aws_sns.publish(
            TopicArn = self.args['ALARM_SNS'],
            Subject = '【Fatal】ETL処理中にFatalエラーが発生【'+self.customer_id+'】',
            Message = msg
        )
    
    
    ##########################################################
    #ログテーブルにデータを登録する
    ##########################################################
    def InsetToDwhLog(self, code, level, message, file_name=None, key=None, item=None, occurred_key=None, total_num=None, normal_num=None, error_num=None):
        if not key:
            key = 'null'
        else:
            key = '\''+key+'\''
        if not total_num and total_num != 0:
            total_num = 'null'
        if not normal_num and normal_num != 0:
            normal_num = 'null'
        if not error_num and error_num != 0:
            error_num = 'null'

        if file_name:
            file_name = '\''+file_name.replace('\'', '\\\'')+'\''
        else:
            file_name = 'null'

        if item:
            item = '\''+item+'\''
        else:
            item = 'null'
        
        message = message.replace("'", "\\'")

        try:
            with self.dwh_connection.cursor() as cur:
                sql = 'INSERT INTO cmn.dtb_etl_log (process_id, level, message_cd, message, target_file, item, occurred_key, total_num, normal_num, error_num, seq, create_id, create_dtt, last_upd_id, last_upd_dtt, customer_id)'\
                    + ' VALUES '\
                    + '(\'{}\', \'{}\', \'{}\', \'{}\', {}, {}, {}, {}, {}, {}, {}, \'{}\', {}, \'{}\', {}, \'{}\')'
                sql = sql.format(
                    self.process_id,    #process_id
                    level,              #level
                    code,               #message_cd
                    message[0:self.DB_LOG_MAX_LENGTH-1],            #message(最大文字数制限)
                    file_name,          #target_file
                    item,               #item
                    key,      #occurred_line
                    total_num,          #total_num
                    normal_num,         #normal_num
                    error_num,          #error_num
                    self.GetLogSequence(),#seq
                    'ETL',              #create_id
                    self.SQL_NOW,       #create_dtt
                    'ETL',              #last_upd_id
                    self.SQL_NOW,       #last_upd_dtt
                    self.customer_id    #customer_id
                )
                count = cur.execute(
                    sql
                )
                self.dwh_connection.commit()

        except Exception as e:
            self.Logging('ログテーブルへの書き込みに失敗', 'error') #ログテーブル書き込み失敗時は何もしない
    

    ##########################################################
    #Info004用文言生成
    ##########################################################
    def GenerateInfo004(self, is_insert, is_upsert, is_delete):
        text = ''

        if is_insert:
            text = text + '登録'
        elif is_upsert:
            text = text + '登録/更新'
        if text == '' and is_delete:
            text = text + '削除'
        elif is_delete:
            text = text + '/削除'
        if text == '':
            text = text + '処理'

        return text


    ##########################################################
    #ログ出力用にキー項目とデータを整形した文字列を生成する
    #keys : 対象のキー項目名(配列)
    #data : 対象のデータ(pd.dataframe)
    ##########################################################
    def FormatKeyData(self, keys, data):
        strKeyData = ''
        arrKeyData = []
        for key in keys:
            arrKeyData.append(key.upper()+'='+data[key.lower()])

        strKeyData = ','.join(arrKeyData)
        return strKeyData
        
    ##########################################################
    #ログを整形して出力する
    #level : Info, Fatal, Error, Warn
    ##########################################################
    def GetLogSequence(self):
        self.log_seq = self.log_seq+1
        return self.log_seq

    ##########################################################
    #ログを整形して出力する
    #level : Info, Fatal, Error, Warn
    ##########################################################
    def FormatLogMessage(self, code, message, file_name=None, key=None, join_sql=False, str_key_sql = False):
        occurred_key = key
        #現在日時
        dt_now = datetime.datetime.now(self.tz);
        str_dt_now = dt_now.strftime('%Y/%m/%d %H:%M:%S')

        #ログレベルをロギング関数に合わせる
        if code.find('Info') != -1:
            level = 'Info'
            log_level = 'info'
        elif code.find('Warn') != -1:
            level = 'Warn'
            log_level = 'warning'
        elif code.find('Error') != -1:
            level = 'Error'
            log_level = 'error'
        elif code.find('Fatal') != -1:
            level = 'Fatal'
            log_level = 'error'
        else:
            level = 'Info'
            log_level = 'info'

        if join_sql:
            message = '\'【'+level.ljust(5)+'】 ['+str_dt_now+'] \'||'+message
        else:
            message = '【'+level.ljust(5)+'】 ['+str_dt_now+'] '+message
            
    
        #ファイル名がある場合にログの後ろに付け加える
        if file_name:
            if join_sql:
                message = message+'||\'('+file_name+')\''
            else:
                message = message+'('+file_name+')'

        #エラー行がある場合にログの後ろに付け加える
        if key:
            if join_sql:
                if str_key_sql:
                    message = message+'||\'(\'||'+key+'||\')\''
                else:
                    message = message+'||\'('+str(key)+')\''
            else:
                message = message+'('+str(key)+')'

        return message
        
    def FormatLogging(self, code, message, file_name=None, key=None, item=None, total_num=None, normal_num=None, error_num=None,write_table=True):
        occurred_key = key

        #ログレベルをロギング関数に合わせる
        if code.find('Info') != -1:
            level = 'Info'
            log_level = 'info'
        elif code.find('Warn') != -1:
            level = 'Warn'
            log_level = 'warning'
        elif code.find('Error') != -1:
            level = 'Error'
            log_level = 'error'
        elif code.find('Fatal') != -1:
            level = 'Fatal'
            log_level = 'error'
        else:
            level = 'Info'
            log_level = 'info'

        message = self.FormatLogMessage(code, message, file_name, key)
        
        #---------------------------------------------------------
        #ログテーブルへの書き込み
        #---------------------------------------------------------
        if write_table:
            self.InsetToDwhLog(
                code=code,
                level=level,
                message=message,
                file_name=file_name,
                key=key,
                item=item,
                occurred_key=occurred_key,
                total_num=total_num,
                normal_num=normal_num,
                error_num=error_num
            )

        #---------------------------------------------------------
        #エラー発生時のメール送信(初回のみ)
        #---------------------------------------------------------
        if level == 'Error' and not self.is_sent_error_mail:
            #エラーメール送信はファイル毎に初回のみ
            self.is_sent_error_mail = True
            self.SendErrorMail(file_name)
        
        if level == 'Fatal' and not self.is_sent_fatal_mail:
            #Fatalメール送信はファイル毎に初回のみ
            self.is_sent_fatal_mail = True
            self.SendFatalMail(file_name)
    
        self.Logging(message, log_level)
    
    ##########################################################
    #DWHへの接続確認
    ##########################################################
    def ChkDwhConnection(self):
        ret = True
        job_ip = ''
        dwh_ip = ''

        for i in range(self.DWH_RETRY_MAX+1):
            try:
                #実行中のGlueのIPアドレス
                job_ip = socket.gethostbyname(socket.gethostname())

                #DWHのIPアドレス
                dwh_ip = socket.gethostbyname(self.args['DWH_DB_HOST'])
                #DWHへのコネクション生成
                self.dwh_connection =  redshift_connector.connect(
                    host=self.args['DWH_DB_HOST'],
                    database=self.args['DWH_DB_NAME'],
                    port=int(self.args['DWH_DB_PORT']),
                    user=self.dwh_db_secrets['username'],
                    password=self.dwh_db_secrets['password']
                )
                
                self.FormatLogging('Info008', 'sourceIP: '+job_ip+'  destinationIP: '+dwh_ip)
                break
            except Exception as e:
                if i < self.DWH_RETRY_MAX:
                    #規定回数以内であればリトライ
                    self.FormatLogging('Info009', 'DBへの接続に失敗したためリトライします。(sourceIP: '+job_ip+'  destinationIP: '+dwh_ip+')')
                    time.sleep(self.DWH_RETRY_WAIT)
                else:
                    #規定回数を超えた場合はFatalで終了
                    self.FormatLogging('Fatal008', 'DBへの接続に失敗しました。(sourceIP: '+job_ip+'  destinationIP: '+dwh_ip+')')
                    ret = False

        return ret

    ##########################################################
    #Aurora DF取得
    ##########################################################
    def GetAuroraDataframe(self, schema, table):
        try:
            #Aurora　Postgresqlへのコネクション生成
            #self.aurora_url = 'jdbc:{}://{}:{}/{}'.format(
            #     'postgresql',
            #     self.args['AURORA_HOST'],
            #     self.args['AURORA_PORT'],
            #     self.args['AURORA_DB_NAME']
            #)

            # print(self.Auorra_db_secrets)

            # params = {
            #      'user': self.Auorra_db_secrets['username'],
            #      'password': self.Auorra_db_secrets['password']
            # }

            # データベースに接続
            conn = psycopg2.connect(
                dbname=self.args['AURORA_DB_NAME'],
                user=self.Auorra_db_secrets['username'],
                password=self.Auorra_db_secrets['password'],
                host=self.args['AURORA_HOST'],
                port=self.args['AURORA_PORT']
            )

            try:
                # カーソルの作成
                cur = conn.cursor()
                
                # 現在の日時と24時間前の日時を取得
                now = datetime.datetime.now(self.tz)
                term = now - datetime.timedelta(hours=int(self.args['TARGET_TERM_HOUR']))
                
                # SQLクエリの実行
                cur.execute('SELECT * FROM '+schema+'.'+table+' WHERE \''+term.strftime('%Y-%m-%d %H:%M:%S')+'\' <= update_date_time;')
                # cur.execute('SELECT * FROM '+schema+'.'+table+' LIMIT 1000;')
                
                # データの取得
                cols = [col.name for col in cur.description]
                res = pd.DataFrame(cur.fetchall(),columns = cols)
                
            finally:
                # カーソルと接続のクローズ
                cur.close()
                conn.close()

            return res

        except Exception as e:
            raise e
    
    ##########################################################
    #S3内のuploadディレクトリ内のファイルの一覧を取得する
    ##########################################################
##    def TargetFile(self):
##        list = []
##        try:
##            s3_resource = self.aws_session.resource('s3')
##            a = s3_resource.Bucket(self.args['DATA_LAKE_S3_BUCKET']).objects.filter(Prefix=self.UPLOAD_FILE_DIR+'/')
##            
##            b = [k.key for k in a]
##
##            for content in b:
##                #最後が/で終わる場合はフォルダなので除外
##                if not content[-1] == '/':
##                    list.append(content)
##            
##        except Exception as e:
##            #ファイルが存在しない
##            list = []
##        
##        return list
    
    ##########################################################
    #制御マスタから処理対象のテーブル一覧を取得する
    ##########################################################
    def TargetTable(self):
        try:
            with self.dwh_connection.cursor() as cur:
            
                count = cur.execute(
                    'SELECT DISTINCT aurora_table FROM {};'.format(
                        'cfg.mst_etl_aurora_control'
                    )
                )

                ret = cur.fetch_dataframe()
                return ret

        except Exception as e:
            print(e)
            return pd.DataFrame()
        
        return list

    ##########################################################
    #ワークテーブルのERROR＿FLG更新
    ##########################################################
    def UpdateErrorFlg(self, target_schema, target_column, rows):
        join_row = ",".join(rows)
        dbname = target_schema+'.'+target_column

        try:
            with self.dwh_connection.cursor() as cur:
                sql = 'UPDATE {} SET ERROR_FLG = {} WHERE ROW IN ({});'

                cur.execute(
                    sql.format(
                        dbname,
                        'True',
                        join_row
                    )
                )
        except Exception as e:
            raise e

        return True
    
    ##########################################################
    #制御マスタの取得
    ##########################################################
    def ValidMstControl(self, file_name):
        ret = True
        #---------------------------------------------------------
        #更新時キーチェック
        #---------------------------------------------------------
        chk = 0
        try:
            with self.dwh_connection.cursor() as cur:
            
                count = cur.execute(
                    'SELECT COUNT(1) AS count FROM {} WHERE aurora_table = \'{}\' AND search_key = {};'.format(
                        'cfg.mst_etl_aurora_control',
                        self.file_code,
                        'true'
                    )
                )
                cnt = cur.fetchone()
                chk = cnt[0]

        except Exception as e:
            raise e
        
        if int(chk) == 0:
            ret = False
            #エラーログの出力
            self.FormatLogging('Fatal010', '制御マスタに更新時キーが定義されていません。', file_name)

        #---------------------------------------------------------
        #妥当性検査項目
        #---------------------------------------------------------
#        chk = 0
#        try:
#            with self.dwh_connection.cursor() as cur:
#                count = cur.execute(
#                    'SELECT COUNT(1) AS count FROM {} WHERE aurora_table = \'{}\' AND validation <> \'\' AND NOT validation ~ \'{}\';'.format(
#                        'cfg.mst_etl_aurora_control',
#                        self.file_code,
#                        '^((Required|MaxLength|MinLength|MaxValue|MinValue|,)*)+$'
#                    )
#                )
#                cnt = cur.fetchone()
#                chk = cnt[0]
#
#        except Exception as e:
#            raise e
#        
#        if int(chk) != 0:
#            ret = False
#            #エラーログの出力
#            self.FormatLogging('Fatal011', '制御マスタの妥当性検査項目に未定義のパラメータが設定されています。', file_name)

        #---------------------------------------------------------
        #最大桁チェック
        #---------------------------------------------------------
        chk = 0
        try:
            with self.dwh_connection.cursor() as cur:
                count = cur.execute(
                    'SELECT COUNT(1) AS count FROM {} WHERE aurora_table = \'{}\' AND validation LIKE \'%{}%\' AND max_length IS NULL;'.format(
                        'cfg.mst_etl_aurora_control',
                        self.file_code,
                        'MaxLength'
                    )
                )
                cnt = cur.fetchone()
                chk = cnt[0]

        except Exception as e:
            raise e
        
        if int(chk) != 0:
            ret = False
            #エラーログの出力
            self.FormatLogging('Fatal012', '制御マスタに最大桁が未定義のデータがあります。', file_name)

        #---------------------------------------------------------
        #最小桁チェック
        #---------------------------------------------------------
        chk = 0
        try:
            with self.dwh_connection.cursor() as cur:
                count = cur.execute(
                    'SELECT COUNT(1) AS count FROM {} WHERE aurora_table = \'{}\' AND validation LIKE \'%{}%\' AND min_length IS NULL;'.format(
                        'cfg.mst_etl_aurora_control',
                        self.file_code,
                        'MinLength'
                    )
                )
                cnt = cur.fetchone()
                chk = cnt[0]

        except Exception as e:
            raise e
        
        if int(chk) != 0:
            ret = False
            #エラーログの出力
            self.FormatLogging('Fatal013', '制御マスタに最小桁が未定義のデータがあります。', file_name)

        #---------------------------------------------------------
        #最大値チェック
        #---------------------------------------------------------
        chk = 0
        try:
            with self.dwh_connection.cursor() as cur:
                count = cur.execute(
                    'SELECT COUNT(1) AS count FROM {} WHERE aurora_table = \'{}\' AND validation LIKE \'%{}%\' AND max_value IS NULL;'.format(
                        'cfg.mst_etl_aurora_control',
                        self.file_code,
                        'MaxValue'
                    )
                )
                cnt = cur.fetchone()
                chk = cnt[0]

        except Exception as e:
            raise e
        
        if int(chk) != 0:
            ret = False
            #エラーログの出力
            self.FormatLogging('Fatal016', '制御マスタに最大値が未定義のデータがあります。', file_name)

        #---------------------------------------------------------
        #最小値チェック
        #---------------------------------------------------------
        chk = 0
        try:
            with self.dwh_connection.cursor() as cur:
                count = cur.execute(
                    'SELECT COUNT(1) AS count FROM {} WHERE aurora_table = \'{}\' AND validation LIKE \'%{}%\' AND min_value IS NULL;'.format(
                        'cfg.mst_etl_aurora_control',
                        self.file_code,
                        'MinValue'
                    )
                )
                cnt = cur.fetchone()
                chk = cnt[0]

        except Exception as e:
            raise e
        
        if int(chk) != 0:
            ret = False
            #エラーログの出力
            self.FormatLogging('Fatal016', '制御マスタに最小値が未定義のデータがあります。', file_name)


        return ret
    
    ##########################################################
    #制御マスタ取得
    ##########################################################
    def GetMstControl(self, file_name):
        ret = []
        try:
            with self.dwh_connection.cursor() as cur:
                count = cur.execute(
                    'SELECT * FROM {} WHERE aurora_table = \'{}\' ORDER BY seq asc;'.format(
                        'cfg.mst_etl_aurora_control',
                        self.file_code
                    )
                )
                # cols = [col.name for col in cur.description]
                # ret = pd.DataFrame(cur.fetchall(),columns = cols)

                ret = cur.fetch_dataframe()

        except Exception as e:
            raise e
        
        return ret
    
    ##########################################################
    #対象テーブルのトランケート
    ##########################################################
    def TruncateTable(self, schema, talbe_name):
        ret = True
        try:
            with self.dwh_connection.cursor() as cur:
                cur.execute(
                    'TRUNCATE TABLE {};'.format(
                        schema+'.'+talbe_name
                    )
                )
                self.dwh_connection.commit()
        except Exception as e:
            raise e
        
        return ret


    ##########################################################
    #ワークテーブル取り込み
    ##########################################################
    def ImportWorkTable(self, controls, file_name):
        try:
            #処理対象の各種スキーマ、テーブル名を保持

            self.target_work_schema = controls.loc[0, 'target_work_schema']
            self.target_work_table = controls.loc[0, 'target_work_table']
            self.target_schema = controls.loc[0, 'target_schema']
            self.target_table = controls.loc[0, 'target_table']

            #---------------------------------------------------------
            #ワークテーブルのリセット
            #---------------------------------------------------------
            if self.IS_DEBUG:
                self.LoggingWithTime('Start truncate work table', 'info')
            self.TruncateTable(self.target_work_schema, self.target_work_table)


            #---------------------------------------------------------
            #Auroraからデータを取得
            #---------------------------------------------------------
            if self.IS_DEBUG:
                self.LoggingWithTime('Start get aurora data', 'info')

            #dynamic  frameの取得
            df = self.GetAuroraDataframe(controls.loc[0, 'aurora_schema'].lower(), controls.loc[0, 'aurora_table'].lower())
            # # 現在の日時と24時間前の日時を取得
            # now = datetime.datetime.now(self.tz)
            # term = now - datetime.timedelta(hours=int(self.args['TARGET_TERM_HOUR']))
            # filtered_dynamic_frame = df.filter(
            #                           f = lambda x: term <= datetime.strptime(x["update_date_time"], '%Y-%m-%d %H:%M:%S') <= now)
            # # filtered_dynamic_frame = df.toDF()
            # print("filtered_dynamic_frame:count")
            # print(filtered_dynamic_frame.count())
            
            if df.empty:
                raise Exception('Info010')

            #dfを分割してバルクインサート
            chunks = np.array_split(df, np.arange(self.MAX_BULK_INSERT, len(df), self.MAX_BULK_INSERT))
            
            for chunk in chunks:

                #データフレームをループ処理でINSERT文を生成
                insert_values = []
                for idx,row in chunk.iterrows():
                    insert_cols = []
                    for c_idx,c_row in controls.iterrows():
                        wk_col = str(c_row['target_column'])

                        #aur_colが空文字出ない場合はダブルクォーテーションを付ける
                        if row[str(c_row['target_column']).lower()] is None or row[str(c_row['target_column']).lower()] == "" or row[str(c_row['target_column']).lower()] == "nan" or row[str(c_row['target_column']).lower()] == "NaT"or str(row[str(c_row['target_column']).lower()]) == "nan" or str(row[str(c_row['target_column']).lower()]) == "NaT":
                            aur_col = "\'\'"
                        else:
                            # 文字列の末尾がスラッシュであるかチェック
                            s = str(row[str(c_row['target_column']).lower()]).replace("\\", "").replace("'", "\\'")
                            # if s.endswith('/'):
                            #     # 末尾のスラッシュをエスケープ
                            #     s = s[:-1] + '\\/'
                            aur_col = "'" + s + "'"

                        insert_cols.append(aur_col)
                    #insert_colsの値をカンマ区切りで結合
                    joind_cols = ','.join(insert_cols)
                    insert_values.append('('+joind_cols+')')
                
                #insert_valuesの値をカンマ区切りで結合
                insert_value = ','.join(insert_values)

                #DFからSQLを生成してワークテーブルにデータを取り込む
                try:
                    with self.dwh_connection.cursor() as cur:
                        sql = 'INSERT INTO {} ({}) VALUES {};'.format(
                            self.target_work_schema+'.'+self.target_work_table,
                            ','.join(controls['target_column']),
                            insert_value
                        )

                        cur.execute(
                            sql
                        )

                    self.dwh_connection.commit()
                except Exception as e:
                    self.dwh_connection.rollback()
                    self.FormatLogging('Fatal007', ' 対象テーブルの取り込みに失敗しました。【'+str(e)+'】', file_name, 0)
                    raise Exception('Fatal007')
            
            try:
                with self.dwh_connection.cursor() as cur:
                    if self.IS_DEBUG:
                        self.LoggingWithTime('Start insert with row', 'info')
                    
                    dbname = self.target_work_schema+'.'+self.target_work_table

                    #ワークテーブルへのシステム項目の割り当て
                    sql = 'UPDATE {} SET '  \
                        + 'PROCESS_DIV={}, CREATE_ID={}, CREATE_DTT={}, LAST_UPD_ID={}, LAST_UPD_DTT={}, ERROR_FLG={};' 
                    count = cur.execute(
                            sql.format(
                                dbname,
                                '\'U\'',
                                '\'ETL\'',
                                self.SQL_NOW,
                                '\'ETL\'',
                                self.SQL_NOW,
                                'False',
                        )
                    )

                    if self.IS_DEBUG:
                        self.LoggingWithTime('Finish import work process', 'info')

                self.dwh_connection.commit()
            except Exception as e:
                self.dwh_connection.rollback()
                raise e
            
        except Exception as e:
            self.dwh_connection.rollback()
            raise e

    ##########################################################
    #それぞれの処理区分の数のチェック
    ##########################################################
    def GetProcessDivNum(self, controls, process_div='U', count_error=False):
        ret = 0
        try:
            with self.dwh_connection.cursor() as cur:
                if process_div == 'I':
                    sql = 'SELECT COUNT(1) AS count FROM {} WHERE PROCESS_DIV NOT IN (\'L\', \'U\', \'D\', \'\') AND PROCESS_DIV IS NOT null'
                else:
                    sql = 'SELECT COUNT(1) AS count FROM {} WHERE PROCESS_DIV = \'{}\''

                if count_error:
                    sql = sql+';'
                else :
                    sql = sql+' AND ERROR_FLG = False;'
                format_sql = sql.format(
                    self.target_work_schema+'.'+self.target_work_table,
                    process_div
                )
                count = cur.execute(
                    format_sql
                )
                cnt = cur.fetchone()
                ret = cnt[0]

        except Exception as e:
            raise e

        return ret
    
    ##########################################################
    #テーブルの件数の取得
    ##########################################################
    def GetWorkNum(self, table_name):
        ret = 0
        try:
            with self.dwh_connection.cursor() as cur:
                count = cur.execute(
                    'SELECT COUNT(1) AS count FROM {};'.format(
                        table_name
                    )
                )
                cnt = cur.fetchone()
                ret = cnt[0]

        except Exception as e:
            raise e

        return ret

    ##########################################################
    #テーブルの件数の取得(ERROR_FLG)
    ##########################################################
    def GetWorkNumError(self, table_name, error_flg=True):
        ret = 0
        try:
            with self.dwh_connection.cursor() as cur:
                if error_flg:
                    count = cur.execute(
                        'SELECT COUNT(1) AS count FROM {} WHERE ERROR_FLG = {};'.format(
                            table_name,
                            str(error_flg)
                        )
                    )
                else:
                    count = cur.execute(
                        'SELECT COUNT(1) AS count FROM {} WHERE PROCESS_DIV IN (\'L\',\'U\',\'D\') AND ERROR_FLG = {};'.format(
                            table_name,
                            str(error_flg)
                        )
                    )
                cnt = cur.fetchone()
                ret = cnt[0]

        except Exception as e:
            raise e

        return ret

    ##########################################################
    #ワークテーブルの件数取得
    ##########################################################
    def GetWorkDivNum(self, controls, TARGET='T'):
        ret = 0
        try:
            with self.dwh_connection.cursor() as cur:
                count = cur.execute(
                    'SELECT COUNT(1) AS count FROM {} WHERE PROCESS_DIV = \'{}\' AND ERROR_FLG = False;'.format(
                        self.target_work_schema+'.'+self.target_work_table,
                        process_div
                    )
                )
                cnt = cur.fetchone()
                ret = cnt[0]

        except Exception as e:
            raise e

        return ret

    ##########################################################
    #データチェック
    ##########################################################
    def ValidData(self, controls, file_name):
        work_dbname = self.target_work_schema+'.'+self.target_work_table
        work_dbname = self.target_work_schema+'.'+self.target_work_table
        return_status = True
        
        try:
            with self.dwh_connection.cursor() as cur:
                #エラー行生成に使用するキー項目の取得
                target_key = []
                key_row = controls[controls.search_key == True]
                for idx,row in key_row.iterrows():
                    target_key.append(row['target_column'])
                target_key_join = ','.join(target_key)

                #エラーログの出力
                key_str = key_row.loc[:,'target_column'].to_list()
                key_join = "と".join(key_str)


                #---------------------------------------------------------
                #顧客IDチェック
                #---------------------------------------------------------  
                chk = 0
                # count = cur.execute(
                #     'SELECT COUNT(1) AS count FROM {} WHERE customer_id <> \'{}\' AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\' ;'.format(
                #         work_dbname,
                #         self.customer_id
                #     )
                # )
                # cnt = cur.fetchone()
                # chk = cnt[0]

                # if int(chk) != 0:
                #     ret = False
                #     #エラーログの出力
                #     self.FormatLogging('Fatal003', '顧客IDが不正です。', file_name)

                #     #ワークテーブルのTRUNCATE
                #     self.TruncateTable(self.target_work_schema,self.target_work_table)
                #     raise Exception('Fatal003')
                
                #---------------------------------------------------------
                #型妥当性チェック
                #---------------------------------------------------------  
                #制御マスタに定義されているカラムの分だけチェック
#                for idx,row in controls.iterrows():
#                    res = cur.execute(
#                        'SELECT data_type,numeric_precision,numeric_scale FROM information_schema.columns WHERE table_schema = \'{}\' and table_name = \'{}\' and column_name = \'{}\';'.format(
#                            row['target_schema'].lower(),
#                            row['target_table'].lower(),
#                            row['target_column'].lower()
#                        )
#                    )
#                    cnt = cur.fetchone()
#                    column_type = cnt[0]
#
#                    check_pattern = None
#                    
#                    if column_type.lower() in ['smallint', 'integer', 'bigint', 'int2', 'int4', 'int8']:
#                        #整数型
#                        check_pattern = self.REGEX_INT
#                    elif column_type.lower() in ['decimal', 'real', 'double', 'precision', 'double precision', 'numeric', 'float4', 'float8', 'float']:
#                        #真数、小数
#                        check_pattern = self.REGEX_DECIMAL
#                    elif column_type.lower() in ['boolean', 'bool']:
#                        #BOOL
#                        check_pattern = self.REGEX_BOOL
#                    #elif column_type.lower() in ['char', 'verchar', 'character', 'nchar', 'bpchar', 'character varying', 'nvarchar', 'text']:
#                        #文字列
#                        #文字列は特にチェック無し
#                    elif column_type.lower() in ['char', 'character']:
#                        #シングルバイト文字列
#                        check_pattern = self.REGEX_SINGLE_CHAR
#                    elif column_type.lower() in ['date']:
#                        #日付
#                        check_pattern = self.REGEX_DATE
#                    elif column_type.lower() in ['timestamp without time zone', 'timestamp with time zone']:
#                        #日時
#                        check_pattern = self.REGEX_DATETIME
#                    elif column_type.lower() in ['time', 'timetz', 'time without time zone', 'time with time zone']:
#                        #時間
#                        check_pattern = self.REGEX_TIME

                    
                    #数値型のオーバーフローチェック
#                    over_flow_where = ''
#                    if column_type.lower() in ['smallint','int2']:
#                        over_flow_where = ' OR (CASE WHEN LEN({}) < 20 THEN CASE WHEN {} ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN CAST({} AS DECIMAL(20,0))-CAST(32767 AS DECIMAL(20,0))>0 OR CAST({} AS DECIMAL(20,0))+CAST(32768 AS DECIMAL(20,0))<0 ELSE FALSE END ELSE TRUE END) '.format(row['target_column'].lower(),row['target_column'].lower(),row['target_column'].lower(),row['target_column'].lower())
#                    elif column_type.lower() in ['integer','int4']:
#                        over_flow_where = ' OR (CASE WHEN LEN({}) < 20 THEN CASE WHEN {} ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN CAST({} AS DECIMAL(20,0))-CAST(2147483647 AS DECIMAL(20,0))>0 OR CAST({} AS DECIMAL(20,0))+CAST(2147483648 AS DECIMAL(20,0))<0 ELSE FALSE END ELSE TRUE END) '.format(row['target_column'].lower(),row['target_column'].lower(),row['target_column'].lower(),row['target_column'].lower())
#                    elif column_type.lower() in ['bigint','int8']:
#                        over_flow_where = ' OR (CASE WHEN LEN({}) < 20 THEN CASE WHEN {} ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN CAST({} AS DECIMAL(20,0))-CAST(9223372036854775807 AS DECIMAL(20,0))>0 OR CAST({} AS DECIMAL(20,0))+CAST(9223372036854775808 AS DECIMAL(20,0))<0 ELSE FALSE END ELSE TRUE END) '.format(row['target_column'].lower(),row['target_column'].lower(),row['target_column'].lower(),row['target_column'].lower())
#                    elif column_type.lower() in ['decimal', 'numeric']:
#                        precision = int(cnt[1])
#                        scale = int(cnt[2])
#                        if scale == 0 :
#                            CHECK_OVER_FLOW_DECIMAL = "^-?[0-9]{1,"+str(int(precision-scale))+"}$"
#                        else:
#                            CHECK_OVER_FLOW_DECIMAL = "^-?[0-9]{1,"+str(int(precision-scale))+"}(\\\.[0-9]{1,"+str(int(scale))+"})?$"
#
#                        over_flow_where = ' OR (CASE WHEN LEN({}) < 20 THEN CASE WHEN {} ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN NOT {} ~ \'{}\' ELSE FALSE END ELSE TRUE END) '.format(row['target_column'].lower(),row['target_column'].lower(),row['target_column'].lower(),CHECK_OVER_FLOW_DECIMAL)
#                    elif column_type.lower() in ['float4', 'real', 'double', 'precision', 'double precision', 'float8', 'float']:
#                        max_val = '9'*30
#                        over_flow_where = ' OR (CASE WHEN LEN({}) < 20 THEN CASE WHEN {} ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN CAST({} AS FLOAT)-CAST({} AS FLOAT)>0 OR CAST({} AS FLOAT)+CAST({} AS FLOAT)<0 ELSE FALSE END ELSE TRUE END) '.format(row['target_column'].lower(),row['target_column'].lower(),row['target_column'].lower(),max_val,row['target_column'].lower(),max_val)

                    
                    #エラー対象の抽出
#                    if check_pattern:
#                        res = None
#                        sql = 'SELECT * FROM {}.{} WHERE ((NOT {} ~ \'{}\' AND {} <> \'\') {}) AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\';'.format(
#                                row['target_work_schema'].lower(),
#                                row['target_work_table'].lower(),
#                                row['target_column'].lower(),
#                                check_pattern,
#                                row['target_column'].lower(),
#                                over_flow_where
#                            )
#
#                        cur.execute(
#                            sql
#                        )
#                        # cols = [col.name for col in cur.description]
#                        # res = pd.DataFrame(cur.fetchall(),columns = cols)
#
#                        res = cur.fetch_dataframe()
#                        if res is None:
#                            res = pd.DataFrame()
#
#                        if not res.empty:
#                            ret = False
#                            #エラーログの出力
#                            for index,data in res.iterrows():
#                                strKey = self.FormatKeyData(target_key, data)
#                                self.FormatLogging('Error010', row['target_column']+'は'+column_type+'型の必要があります。', file_name, strKey, row['target_column'], write_table=False)
                            
                            #エラーログの出力
#                            key_str = key_row.loc[:,'target_column'].to_list()
#                            key_join = "と".join(key_str)
                            
                            #key項目の処理
#                            error_key_column = []
#                            for key in key_str:
#                                error_key_column.append('\''+key.upper()+'=\'||'+'WT.'+key.lower())
                            
#                            error_key_column_join = '||\',\'||'.join(error_key_column)

#                            sql_format_trim = sql.strip(';')

                            #エラーログテーブルへの登録
#                            log_sql = 'INSERT INTO cmn.dtb_etl_log (process_id, level, message_cd, message, target_file, item, occurred_key, seq, create_id, create_dtt, last_upd_id, last_upd_dtt, customer_id)'\
#                                        + ' SELECT \'{}\', \'{}\', \'{}\', {}, \'{}\', \'{}\', {}, {}, \'{}\', {}, \'{}\', {}, \'{}\'  FROM ({}) AS WT;'

#                            message = self.FormatLogMessage('Error010', '\''+row['target_column']+'は'+column_type+'型の必要があります。'+'\'', file_name, error_key_column_join, join_sql=True, str_key_sql=True)
#                            format_log_sql = log_sql.format(
#                                self.process_id,
#                                'Error',
#                                'Error010',
#                                message,
#                                file_name,
#                                row['target_column'],
#                                error_key_column_join,
#                                self.GetLogSequence(),
#                                'ETL',              #create_id
#                                self.SQL_NOW,       #create_dtt
#                                'ETL',              #last_upd_id
#                                self.SQL_NOW,       #last_upd_dtt
#                                self.customer_id,    #customer_id
#                                sql_format_trim
#                            )
#                            cur.execute(
#                                    format_log_sql
#                                )

#                            #エラーフラグの更新
#                            cur.execute(
#                                'UPDATE {}.{} SET ERROR_FLG=True WHERE ((NOT {} ~ \'{}\' AND {} <> \'\') {}) AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\';'.format(
#                                    row['target_work_schema'].lower(),
#                                    row['target_work_table'].lower(),
#                                    row['target_column'].lower(),
#                                    check_pattern,
#                                    row['target_column'].lower(),
#                                    over_flow_where
#                                )
#                            )

                #---------------------------------------------------------
                #キー重複チェック
                #---------------------------------------------------------
                #キー項目の抽出
                key_sql = ''
                keys_sql = []
                for i,data in key_row.iterrows():
                    keys_sql.append('DUPLICATE_DATA.{} = ORG.{}'.format(data['target_column'],data['target_column']))

                key_sql = ' AND '.join(keys_sql)


                sql = 'SELECT ORG.* '\
                    + 'FROM {} AS ORG '\
                    + 'INNER JOIN ( '\
                        + 'SELECT * '\
                            + 'FROM '\
                            + '(SELECT '\
                                + '{}, '\
                                + 'count(1) AS row_cnt '\
                                + 'FROM {} '\
                                + 'WHERE PROCESS_DIV IS NOT NULL AND PROCESS_DIV <> \'\' '\
                            + 'GROUP BY {}) AS KEY_COUNT '\
                        + 'WHERE KEY_COUNT.row_cnt > 1 '\
                    + ') AS DUPLICATE_DATA ON {};'\

                sql_format = sql.format(
                    work_dbname,
                    target_key_join,
                    work_dbname,
                    target_key_join,
                    key_sql
                )
                ret = None
                cur.execute(
                    sql_format
                )
                # cols = [col.name for col in cur.description]
                # res = pd.DataFrame(cur.fetchall(),columns = cols)

                res = cur.fetch_dataframe()
                if res is None:
                    res = pd.DataFrame()

                if not res.empty:
                    rows=[]
                    #エラーログの出力
                    key_str = key_row.loc[:,'target_column'].to_list()
                    key_join = "と".join(key_str)

                    #key項目の処理
                    error_key_column = []
                    for key in key_str:
                        error_key_column.append('\''+key.upper()+'=\'||'+'WT.'+key.lower())
                    
                    error_key_column_join = '||\',\'||'.join(error_key_column)

                    sql_format_trim = sql_format.strip(';')

                    #エラーログテーブルへの登録
                    log_sql = 'INSERT INTO cmn.dtb_etl_log (process_id, level, message_cd, message, target_file, item, occurred_key, seq, create_id, create_dtt, last_upd_id, last_upd_dtt, customer_id)'\
                                + ' SELECT \'{}\', \'{}\', \'{}\', {}, \'{}\', {}, {}, {}, \'{}\', {}, \'{}\', {}, \'{}\'  FROM ('+sql_format_trim+') AS WT;'

                    message = self.FormatLogMessage('Error004', '\''+key_join+'が重複しています。\'', file_name, error_key_column_join, join_sql=True, str_key_sql=True)
                    format_log_sql = log_sql.format(
                        self.process_id,
                        'Error',
                        'Error004',
                        message,
                        file_name,
                        'NULL',
                        error_key_column_join,
                        self.GetLogSequence(),
                        'ETL',              #create_id
                        self.SQL_NOW,       #create_dtt
                        'ETL',              #last_upd_id
                        self.SQL_NOW,       #last_upd_dtt
                        self.customer_id    #customer_id
                    )
                    cur.execute(
                            format_log_sql
                        )


                    for index,error_row in res.iterrows():
                        strKey = self.FormatKeyData(target_key, error_row)
                        self.FormatLogging('Error004', key_join+'が重複しています。', file_name, strKey, write_table=False)

                        #エラー対象のERROR_FLGを更新
                        error_key_row = controls[controls.search_key == True]
                        error_key_where = []
                        for error_idx,err_row in error_key_row.iterrows():
                            error_key_where.append(err_row['target_column'].lower()+'=\''+error_row[err_row['target_column'].lower()]+'\'')
                        error_key_where_join = ' AND '.join(error_key_where)

                        sql = 'UPDATE {} '\
                            'SET error_flg = TRUE '\
                            'WHERE {} '\
                            'AND PROCESS_DIV IS NOT NULL AND PROCESS_DIV <> \'\';'
                        sql=sql.format(
                            work_dbname,
                            error_key_where_join
                        )
                        cur.execute(
                            sql
                        )


                update_num = self.GetProcessDivNum(controls, 'U', count_error=True)
                delete_num = self.GetProcessDivNum(controls, 'D', count_error=True)
                laundering_num = self.GetProcessDivNum(controls, 'L', count_error=True)
                invalid_num = self.GetProcessDivNum(controls, 'I', count_error=True)
                #---------------------------------------------------------
                #処理区分チェック
                #---------------------------------------------------------
                if update_num==0 and delete_num==0 and laundering_num==0 and invalid_num!=0:
                    self.FormatLogging('Fatal018', 'ファイル内に処理区分が設定されていません。', file_name)
                    raise Exception('Fatal018')

                if laundering_num!=0 and (update_num!=0 or delete_num!=0):
                    self.FormatLogging('Fatal019', 'ファイル内の処理区分に「L」と「U」「D」が設定されています。', file_name)
                    raise Exception('Fatal019')

                #U,Dチェック
                if update_num != 0 or delete_num != 0:
                    sql = 'SELECT * FROM {} WHERE PROCESS_DIV NOT IN (\'U\', \'D\') AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\';'
                    sql_format = sql.format(
                        work_dbname
                    )
                    ret = None
                    cur.execute(
                        sql_format
                    )
                    # cols = [col.name for col in cur.description]
                    # res = pd.DataFrame(cur.fetchall(),columns = cols)

                    res = cur.fetch_dataframe()
                    if res is None:
                        res = pd.DataFrame()

                    if not res.empty:
                        #エラーログの出力
                        for index,data in res.iterrows():
                            strKey = self.FormatKeyData(target_key, data)
                            self.FormatLogging('Error015', 'ファイル内の処理区分に「U」「D」以外が設定されています。', file_name, strKey, 'PROCESS_DIV', write_table=False)
                        
                        #エラーログの出力
                        key_str = key_row.loc[:,'target_column'].to_list()
                        key_join = "と".join(key_str)
                        
                        #key項目の処理
                        error_key_column = []
                        for key in key_str:
                            error_key_column.append('\''+key.upper()+'=\'||'+'WT.'+key.lower())
                        
                        error_key_column_join = '||\',\'||'.join(error_key_column)

                        sql_format_trim = sql_format.strip(';')

                        #エラーログテーブルへの登録
                        log_sql = 'INSERT INTO cmn.dtb_etl_log (process_id, level, message_cd, message, target_file, item, occurred_key, seq, create_id, create_dtt, last_upd_id, last_upd_dtt, customer_id)'\
                                    + ' SELECT \'{}\', \'{}\', \'{}\', {}, \'{}\', \'{}\', {}, {}, \'{}\', {}, \'{}\', {}, \'{}\'  FROM ({}) AS WT;'

                        message = self.FormatLogMessage('Error015', '\'ファイル内の処理区分に「U」「D」以外が設定されています。\'', file_name, error_key_column_join, join_sql=True, str_key_sql=True)
                        format_log_sql = log_sql.format(
                            self.process_id,
                            'Error',
                            'Error015',
                            message,
                            file_name,
                            'PROCESS_DIV',
                            error_key_column_join,
                            self.GetLogSequence(),
                            'ETL',              #create_id
                            self.SQL_NOW,       #create_dtt
                            'ETL',              #last_upd_id
                            self.SQL_NOW,       #last_upd_dtt
                            self.customer_id,    #customer_id
                            sql_format_trim
                        )
                        cur.execute(
                                format_log_sql
                            )

                        #エラーフラグの更新
                        cur.execute(
                            'UPDATE {}.{} SET ERROR_FLG=True WHERE PROCESS_DIV NOT IN (\'U\', \'D\') AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\';'.format(
                                row['target_work_schema'].lower(),
                                row['target_work_table'].lower(),
                            )
                        )

                #Lチェック
                elif laundering_num != 0:
                    sql = 'SELECT * FROM {} WHERE PROCESS_DIV NOT IN (\'L\') AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\';'
                    sql_format = sql.format(
                        work_dbname
                    )
                    ret = None
                    cur.execute(
                        sql_format
                    )
                    # cols = [col.name for col in cur.description]
                    # res = pd.DataFrame(cur.fetchall(),columns = cols)

                    res = cur.fetch_dataframe()
                    if res is None:
                        res = pd.DataFrame()

                    if not res.empty:
                        #エラーログの出力
                        for index,data in res.iterrows():
                            strKey = self.FormatKeyData(target_key, data)
                            self.FormatLogging('Error016', 'ファイル内の処理区分に「L」以外が設定されています。', file_name, strKey, 'PROCESS_DIV', write_table=False)
                        
                        #エラーログの出力
                        key_str = key_row.loc[:,'target_column'].to_list()
                        key_join = "と".join(key_str)
                        
                        #key項目の処理
                        error_key_column = []
                        for key in key_str:
                            error_key_column.append('\''+key.upper()+'=\'||'+'WT.'+key.lower())
                        
                        error_key_column_join = '||\',\'||'.join(error_key_column)

                        sql_format_trim = sql_format.strip(';')

                        #エラーログテーブルへの登録
                        log_sql = 'INSERT INTO cmn.dtb_etl_log (process_id, level, message_cd, message, target_file, item, occurred_key, seq, create_id, create_dtt, last_upd_id, last_upd_dtt, customer_id)'\
                                    + ' SELECT \'{}\', \'{}\', \'{}\', {}, \'{}\', \'{}\', {}, {}, \'{}\', {}, \'{}\', {}, \'{}\'  FROM ({}) AS WT;'

                        message = self.FormatLogMessage('Error016', '\'ファイル内の処理区分に「L」以外が設定されています。\'', file_name, error_key_column_join, join_sql=True, str_key_sql=True)
                        format_log_sql = log_sql.format(
                            self.process_id,
                            'Error',
                            'Error016',
                            message,
                            file_name,
                            'PROCESS_DIV',
                            error_key_column_join,
                            self.GetLogSequence(),
                            'ETL',              #create_id
                            self.SQL_NOW,       #create_dtt
                            'ETL',              #last_upd_id
                            self.SQL_NOW,       #last_upd_dtt
                            self.customer_id,    #customer_id
                            sql_format_trim
                        )
                        cur.execute(
                                format_log_sql
                            )

                        #エラーフラグの更新
                        cur.execute(
                            'UPDATE {}.{} SET ERROR_FLG=True WHERE PROCESS_DIV NOT IN (\'L\') AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\';'.format(
                                row['target_work_schema'].lower(),
                                row['target_work_table'].lower(),
                            )
                        )
                


                #---------------------------------------------------------
                #データ妥当性チェック
                #---------------------------------------------------------
                #制御マスタに定義されているカラムの分だけチェック
#                for idx,row in controls.iterrows():
#                    if row['validation']:
#                        #---------------------------------------------------------
#                        #必須チェック
#                        #---------------------------------------------------------  
#                        if 'Required' in row['validation']:
#                            sql = 'SELECT * FROM {} WHERE '
#                            sql = sql.format(work_dbname)
#                            sql = sql + ' {} IS NULL OR {} = \'\'  AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\';'
#                            sql = sql.format(row['target_column'], row['target_column'])
#
#                            res = pd.DataFrame()
#                            cur.execute(
#                                sql
#                            )
#                            # cols = [col.name for col in cur.description]
#                            # res = pd.DataFrame(cur.fetchall(),columns = cols)
#
#                            res = cur.fetch_dataframe()
#                            if res is None:
#                                res = pd.DataFrame()
#
#                            if not res.empty:
#                                rows=[]
#                                #エラーログの出力
#                                for index,error_row in res.iterrows():
#                                    strKey = self.FormatKeyData(target_key, error_row)
#                                    self.FormatLogging('Error003', row['target_column']+'は必須項目です。', file_name, strKey, row['target_column'], write_table=False)
#
#                                #key項目の処理
#                                error_key_column = []
#                                for key in key_str:
#                                    error_key_column.append('\''+key.upper()+'=\'||'+'WT.'+key.lower())
#                                
#                                error_key_column_join = '||\',\'||'.join(error_key_column)
#                                
#                                sql_format_trim = sql.strip(';')
#
#                                #エラーログテーブルへの登録
#                                log_sql = 'INSERT INTO cmn.dtb_etl_log (process_id, level, message_cd, message, target_file, item, occurred_key, seq, create_id, create_dtt, last_upd_id, last_upd_dtt, customer_id)'\
#                                            + ' SELECT \'{}\', \'{}\', \'{}\', {}, \'{}\', \'{}\', {}, {}, \'{}\', {}, \'{}\', {}, \'{}\'  FROM ('+sql_format_trim+') AS WT;'
#
#                                message = self.FormatLogMessage('Error003', '\''+row['target_column']+'は必須項目です。\'', file_name, error_key_column_join, join_sql=True, str_key_sql=True)
#
#                                format_log_sql = log_sql.format(
#                                    self.process_id,
#                                    'Error',
#                                    'Error003',
#                                    message,
#                                    file_name,
#                                    row['target_column'],
#                                    error_key_column_join,
#                                    self.GetLogSequence(),
#                                    'ETL',              #create_id
#                                    self.SQL_NOW,       #create_dtt
#                                    'ETL',              #last_upd_id
#                                    self.SQL_NOW,       #last_upd_dtt
#                                    self.customer_id    #customer_id
#                                )
#                                cur.execute(
#                                        format_log_sql
#                                    )
#
#                                #エラー対象のERROR_FLGを更新
#                                sql = 'UPDATE {} '\
#                                    'SET error_flg = TRUE '\
#                                    'WHERE {} IS NULL OR {} = \'\' '\
#                                    ' AND PROCESS_DIV IS NOT NULL AND PROCESS_DIV <> \'\';'
#                                cur.execute(
#                                    sql.format(
#                                        work_dbname,
#                                        row['target_column'],
#                                        row['target_column']
#                                    )
#                                )
                                    

#                        #---------------------------------------------------------
#                        #最大桁
#                        #---------------------------------------------------------  
#                        if 'MaxLength' in row['validation']:
#                            sql = 'SELECT *,LENGTH({}) AS LEN FROM {} WHERE '
#                            sql = sql.format(row['target_column'], work_dbname)
#                            sql = sql + ' LENGTH({}) > {} AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\';'
#                            sql = sql.format(row['target_column'], int(row['max_length']))
#
#                            res = pd.DataFrame()
#                            cur.execute(
#                                sql
#                            )
#                            # cols = [col.name for col in cur.description]
#                            # res = pd.DataFrame(cur.fetchall(),columns = cols)
#
#                            res = cur.fetch_dataframe()
#                            if res is None:
#                                res = pd.DataFrame()
#
#                            if not res.empty:
#                                rows=[]
#                                #エラーログの出力
#                                for index,error_row in res.iterrows():
#                                    strKey = self.FormatKeyData(target_key, error_row)
#                                    self.FormatLogging('Error006', row['target_column']+'は'+str(int(error_row['len']))+'桁ですが'+str(int(row['max_length']))+'桁以下の必要があります。', file_name, strKey, row['target_column'], write_table=False)
#
#                                #key項目の処理
#                                error_key_column = []
#                                for key in key_str:
#                                    error_key_column.append('\''+key.upper()+'=\'||'+'WT.'+key.lower())
#                                
#                                error_key_column_join = '||\',\'||'.join(error_key_column)
#                                
#                                sql_format_trim = sql.strip(';')
#
#                                #エラーログテーブルへの登録
#                                log_sql = 'INSERT INTO cmn.dtb_etl_log (process_id, level, message_cd, message, target_file, item, occurred_key, seq, create_id, create_dtt, last_upd_id, last_upd_dtt, customer_id)'\
#                                            + ' SELECT \'{}\', \'{}\', \'{}\', {}, \'{}\', \'{}\', {}, {}, \'{}\', {}, \'{}\', {}, \'{}\'  FROM ('+sql_format_trim+') AS WT;'
#                                
#                                message = self.FormatLogMessage('Error006', '\''+row['target_column']+'は\'|| LENGTH(WT.'+row['target_column']+') ||\'桁ですが'+str(int(row['max_length']))+'桁以下の必要があります。\'', file_name, error_key_column_join, join_sql=True, str_key_sql=True)
#
#                                format_log_sql = log_sql.format(
#                                    self.process_id,
#                                    'Error',
#                                    'Error006',
#                                    message,
#                                    file_name,
#                                    row['target_column'],
#                                    error_key_column_join,
#                                    self.GetLogSequence(),
#                                    'ETL',              #create_id
#                                    self.SQL_NOW,       #create_dtt
#                                    'ETL',              #last_upd_id
#                                    self.SQL_NOW,       #last_upd_dtt
#                                    self.customer_id    #customer_id
#                                )
#                                cur.execute(
#                                        format_log_sql
#                                    )
#
#                                #エラー対象のERROR_FLGを更新
#                                sql = 'UPDATE {} '\
#                                    'SET error_flg = TRUE '\
#                                    'WHERE  LENGTH({}) > {} AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\' ;'
#
#                                cur.execute(
#                                    sql.format(
#                                        work_dbname,
#                                        row['target_column'],
#                                        int(row['max_length'])
#                                    )
#                                )
#                                
#
#                        #---------------------------------------------------------
#                        #最小桁
#                        #---------------------------------------------------------  
#                        if 'MinLength' in row['validation']:
#                            sql = 'SELECT *, LENGTH({}) AS LEN FROM {} WHERE '
#                            sql = sql.format(row['target_column'], work_dbname)
#                            sql = sql + ' LENGTH({}) < {} AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\';'
#                            sql = sql.format(row['target_column'], int(row['min_length']))
#
#                            res = pd.DataFrame()
#                            cur.execute(
#                                sql
#                            )
#                            # cols = [col.name for col in cur.description]
#                            # res = pd.DataFrame(cur.fetchall(),columns = cols)
#
#                            res = cur.fetch_dataframe()
#                            if res is None:
#                                res = pd.DataFrame()
#
#                            if not res.empty:
#                                rows=[]
#                                #エラーログの出力
#                                for index,error_row in res.iterrows():
#                                    strKey = self.FormatKeyData(target_key, error_row)
#                                    self.FormatLogging('Error007', row['target_column']+'は'+str(int(error_row['len']))+'桁ですが'+str(int(row['min_length']))+'桁以上の必要があります。', file_name, strKey, row['target_column'], write_table=False)
#                                
#                                #key項目の処理
#                                error_key_column = []
#                                for key in key_str:
#                                    error_key_column.append('\''+key.upper()+'=\'||'+'WT.'+key.lower())
#                                
#                                error_key_column_join = '||\',\'||'.join(error_key_column)
#                                
#                                sql_format_trim = sql.strip(';')
#
#                                #エラーログテーブルへの登録
#                                log_sql = 'INSERT INTO cmn.dtb_etl_log (process_id, level, message_cd, message, target_file, item, occurred_key, seq, create_id, create_dtt, last_upd_id, last_upd_dtt, customer_id)'\
#                                            + ' SELECT \'{}\', \'{}\', \'{}\', {}, \'{}\', \'{}\', {}, {}, \'{}\', {}, \'{}\', {}, \'{}\'  FROM ('+sql_format_trim+') AS WT;'
#
#                                message = self.FormatLogMessage('Error007', '\''+row['target_column']+'は\'|| LENGTH(WT.'+row['target_column']+') ||\'桁ですが'+str(int(row['min_length']))+'桁以上の必要があります。\'', file_name, error_key_column_join, join_sql=True, str_key_sql=True)
#                                format_log_sql = log_sql.format(
#                                    self.process_id,
#                                    'Error',
#                                    'Error007',
#                                    message,
#                                    file_name,
#                                    row['target_column'],
#                                    error_key_column_join,
#                                    self.GetLogSequence(),
#                                    'ETL',              #create_id
#                                    self.SQL_NOW,       #create_dtt
#                                    'ETL',              #last_upd_id
#                                    self.SQL_NOW,       #last_upd_dtt
#                                    self.customer_id    #customer_id
#                                )
#                                cur.execute(
#                                        format_log_sql
#                                    )

#                                #エラー対象のERROR_FLGを更新
#                                sql = 'UPDATE {} '\
#                                    'SET error_flg = TRUE '\
#                                    'WHERE  LENGTH({}) < {} AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\' ;'

#                                cur.execute(
#                                    sql.format(
#                                        work_dbname,
#                                        row['target_column'],
#                                        int(row['min_length'])
#                                    )
#                                )
#                                
#                        
#                        #---------------------------------------------------------
#                        #最大値
#                        #---------------------------------------------------------  
#                        if 'MaxValue' in row['validation']:
#                            sql = 'SELECT *,{} AS VAL FROM {} WHERE '
#                            sql = sql.format(row['target_column'], work_dbname)
#                            sql = sql + 'CASE WHEN LEN({}) < 20 THEN CAST(CASE WHEN {} ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN {} ELSE \'{}\' END AS FLOAT) > {} ELSE FALSE END AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\';'
#                            sql = sql.format(row['target_column'],row['target_column'], row['target_column'], row['max_value'], row['max_value'])
#                            res = pd.DataFrame()
#                            cur.execute(
#                                sql
#                            )
#                            # cols = [col.name for col in cur.description]
#                            # res = pd.DataFrame(cur.fetchall(),columns = cols)
#
#                            res = cur.fetch_dataframe()
#                            if res is None:
#                                res = pd.DataFrame()
#
#                            if not res.empty:
#                                rows=[]
#                                #エラーログの出力
#                                for index,error_row in res.iterrows():
#                                    strKey = self.FormatKeyData(target_key, error_row)
#                                    self.FormatLogging('Error008', row['target_column']+'は'+str(error_row['val'])+'ですが'+str(row['max_value'])+'以下の必要があります。', file_name, strKey, row['target_column'], write_table=False)
#
#                                #key項目の処理
#                                error_key_column = []
#                                for key in key_str:
#                                    error_key_column.append('\''+key.upper()+'=\'||'+'WT.'+key.lower())
#                                
#                                error_key_column_join = '||\',\'||'.join(error_key_column)
#                                
#                                sql_format_trim = sql.strip(';')
#
#                                #エラーログテーブルへの登録
#                                log_sql = 'INSERT INTO cmn.dtb_etl_log (process_id, level, message_cd, message, target_file, item, occurred_key, seq, create_id, create_dtt, last_upd_id, last_upd_dtt, customer_id)'\
#                                            + ' SELECT \'{}\', \'{}\', \'{}\', {}, \'{}\', \'{}\', {}, {}, \'{}\', {}, \'{}\', {}, \'{}\'  FROM ('+sql_format_trim+') AS WT;'
#                                
#                                message = self.FormatLogMessage('Error008', '\''+row['target_column']+'は\'|| WT.'+row['target_column']+' ||\'ですが'+str(row['max_value'])+'以下の必要があります。\'', file_name, error_key_column_join, join_sql=True, str_key_sql=True)
#                                format_log_sql = log_sql.format(
#                                    self.process_id,
#                                    'Error',
#                                    'Error008',
#                                    message,
#                                    file_name,
#                                    row['target_column'],
#                                    error_key_column_join,
#                                    self.GetLogSequence(),
#                                    'ETL',              #create_id
#                                    self.SQL_NOW,       #create_dtt
#                                    'ETL',              #last_upd_id
#                                    self.SQL_NOW,       #last_upd_dtt
#                                    self.customer_id    #customer_id
#                                )
#                                cur.execute(
#                                        format_log_sql
#                                    )
#
#                                #エラー対象のERROR_FLGを更新
#                                sql = 'UPDATE {} '\
#                                    'SET error_flg = TRUE WHERE '\
#                                    'CASE WHEN LEN({}) < 20 THEN CAST(CASE WHEN {} ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN {} ELSE \'{}\' END AS FLOAT) > {} ELSE FALSE END AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\';'
#
#                                cur.execute(
#                                    sql.format(
#                                        work_dbname,
#                                        row['target_column'],row['target_column'], row['target_column'], row['max_value'], row['max_value']
#                                    )
#                                )
                                
                        
                        #---------------------------------------------------------
                        #最小値
                        #---------------------------------------------------------  
                        if 'MinValue' in row['validation']:
                            sql = 'SELECT *, {} AS VAL FROM {} WHERE '
                            sql = sql.format(row['target_column'], work_dbname)
                            sql = sql + 'CASE WHEN LEN({}) < 20 THEN CAST(CASE WHEN {} ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN {} ELSE \'{}\' END AS FLOAT) < {} ELSE FALSE END AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\';'
                            sql = sql.format(row['target_column'],row['target_column'], row['target_column'], row['min_value'], row['min_value'])
                            res = pd.DataFrame()
                            cur.execute(
                                sql
                            )
                            # cols = [col.name for col in cur.description]
                            # res = pd.DataFrame(cur.fetchall(),columns = cols)

                            res = cur.fetch_dataframe()
                            if res is None:
                                res = pd.DataFrame()

                            if not res.empty:
                                rows=[]
                                #エラーログの出力
                                for index,error_row in res.iterrows():
                                    strKey = self.FormatKeyData(target_key, error_row)
                                    self.FormatLogging('Error009', row['target_column']+'は'+str(error_row['val'])+'ですが'+str(row['min_value'])+'以上の必要があります。', file_name, strKey, row['target_column'], write_table=False)

                                #key項目の処理
                                error_key_column = []
                                for key in key_str:
                                    error_key_column.append('\''+key.upper()+'=\'||'+'WT.'+key.lower())
                                
                                error_key_column_join = '||\',\'||'.join(error_key_column)
                                
                                sql_format_trim = sql.strip(';')

                                #エラーログテーブルへの登録
                                log_sql = 'INSERT INTO cmn.dtb_etl_log (process_id, level, message_cd, message, target_file, item, occurred_key, seq, create_id, create_dtt, last_upd_id, last_upd_dtt, customer_id)'\
                                            + ' SELECT \'{}\', \'{}\', \'{}\', {}, \'{}\', \'{}\', {}, {}, \'{}\', {}, \'{}\', {}, \'{}\'  FROM ('+sql_format_trim+') AS WT;'

                                message = self.FormatLogMessage('Error009', '\''+row['target_column']+'は\'|| WT.'+row['target_column']+' ||\'ですが'+str(row['min_value'])+'以上の必要があります。\'', file_name, error_key_column_join, join_sql=True, str_key_sql=True)
                                format_log_sql = log_sql.format(
                                    self.process_id,
                                    'Error',
                                    'Error009',
                                    message,
                                    file_name,
                                    row['target_column'],
                                    error_key_column_join,
                                    self.GetLogSequence(),
                                    'ETL',              #create_id
                                    self.SQL_NOW,       #create_dtt
                                    'ETL',              #last_upd_id
                                    self.SQL_NOW,       #last_upd_dtt
                                    self.customer_id    #customer_id
                                )
                                cur.execute(
                                        format_log_sql
                                    )

                                #エラー対象のERROR_FLGを更新
                                sql = 'UPDATE {} '\
                                    'SET error_flg = TRUE WHERE '\
                                    'CASE WHEN LEN({}) < 20 THEN  CAST(CASE WHEN {} ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN {} ELSE \'{}\' END AS FLOAT) < {} ELSE FALSE END AND PROCESS_DIV IS NOT null AND PROCESS_DIV <> \'\';'

                                cur.execute(
                                    sql.format(
                                        work_dbname,
                                        row['target_column'],row['target_column'], row['target_column'], row['min_value'], row['min_value']
                                    )
                                )
                    

                #---------------------------------------------------------
                #DELETEチェック
                #---------------------------------------------------------
                delete_num = self.GetProcessDivNum(controls, 'D')
                if not delete_num==0:
                    targets = []
                    db = self.target_schema+'.'+self.target_table

                    sql = 'SELECT WK.* FROM {} AS WK LEFT JOIN {} AS PRD ON '
                    sql = sql.format(work_dbname,db)

                    where = []
                    is_first = True
                    for idx,row in controls.iterrows():
                        if row['search_key']:
                            cur.execute(
                                'SELECT data_type FROM information_schema.columns WHERE table_schema = \'{}\' and table_name = \'{}\' and column_name = \'{}\';'.format(
                                    row['target_schema'].lower(),
                                    row['target_table'].lower(),
                                    row['target_column'].lower()
                                )
                            )
                            res = cur.fetchone()
                            column_type = res[0]
                            if column_type.lower() in ['smallint', 'integer', 'bigint', 'int2', 'int4', 'int8']:
                                #整数型
                                if is_first:
                                    sql = sql+ ' CAST(WK.{} AS BIGINT) = PRD.{} '
                                    is_first = False
                                else:
                                    sql = sql+ ' AND CAST(WK.{} AS BIGINT) = PRD.{} '
                                sql = sql.format(row['target_column'],row['target_column'])
                            elif column_type.lower() in ['decimal', 'real', 'double', 'precision', 'numeric', 'float4', 'float8', 'float']:
                                #真数、小数
                                #整数型
                                if is_first:
                                    sql = sql+ ' CAST(WK.{} AS FLOAT) = PRD.{} '
                                    is_first = False
                                else:
                                    sql = sql+ ' AND CAST(WK.{} AS FLOAT) = PRD.{} '
                                sql = sql.format(row['target_column'],row['target_column'])
                            elif column_type.lower() in ['boolean', 'bool']:
                                #BOOL
                                if is_first:
                                    sql = sql+ ' WK.{} = PRD.{} '
                                    is_first = False
                                else:
                                    sql = sql+ ' AND WK.{} = PRD.{} '
                                sql = sql.format(row['target_column'],row['target_column'])
                            elif column_type.lower() in ['date']:
                                #日付
                                if is_first:
                                    sql = sql+ ' CAST(WK.{} AS DATE) = PRD.{} '
                                    is_first = False
                                else:
                                    sql = sql+ ' AND CAST(WK.{} AS DATE) = PRD.{} '
                                sql = sql.format(row['target_column'],row['target_column'])
                            elif column_type.lower() in ['time', 'timetz', 'time without time zone', 'time with time zone', 'timestamp without time zone', 'timestamp with time zone']:
                                #日時
                                if is_first:
                                    sql = sql+ ' CAST(WK.{} AS TIMESTAMP) = PRD.{} '
                                    is_first = False
                                else:
                                    sql = sql+ ' AND CAST(WK.{} AS TIMESTAMP) = PRD.{} '
                                sql = sql.format(row['target_column'],row['target_column'])
                            else:
                                if is_first:
                                    sql = sql+ ' WK.{} = PRD.{} '
                                    is_first = False
                                else:
                                    sql = sql+ ' AND WK.{} = PRD.{} '
                                sql = sql.format(row['target_column'],row['target_column'])

                    sql = sql + ' WHERE PRD.CREATE_ID IS NULL AND WK.PROCESS_DIV = \'D\' ;'

                    res = pd.DataFrame()
                    cur.execute(
                        sql
                    )
                    # cols = [col.name for col in cur.description]
                    # res = pd.DataFrame(cur.fetchall(),columns = cols)

                    res = cur.fetch_dataframe()
                    if res is None:
                        res = pd.DataFrame()

                    if not res.empty:
                        rows=[]
                        #エラーログの出力
                        #key項目の処理
                        error_key_column = []
                        for key in key_str:
                            error_key_column.append('\''+key.upper()+'=\'||'+'WT.'+key.lower())
                        
                        error_key_column_join = '||\',\'||'.join(error_key_column)
                        
                        sql_format_trim = sql.strip(';')

                        #エラーログテーブルへの登録
                        log_sql = 'INSERT INTO cmn.dtb_etl_log (process_id, level, message_cd, message, target_file, item, occurred_key, seq, create_id, create_dtt, last_upd_id, last_upd_dtt, customer_id)'\
                                    + ' SELECT \'{}\', \'{}\', \'{}\', {}, \'{}\', \'{}\', {}, {}, \'{}\', {}, \'{}\', {}, \'{}\'  FROM ('+sql_format_trim+') AS WT;'
                        
                        message = self.FormatLogMessage('Error001', '\'更新対象が存在しません。\'', file_name, error_key_column_join, join_sql=True, str_key_sql=True)
                        format_log_sql = log_sql.format(
                            self.process_id,
                            'Error',
                            'Error001',
                            message,
                            file_name,
                            row['target_column'],
                            error_key_column_join,
                            self.GetLogSequence(),
                            'ETL',              #create_id
                            self.SQL_NOW,       #create_dtt
                            'ETL',              #last_upd_id
                            self.SQL_NOW,       #last_upd_dtt
                            self.customer_id    #customer_id
                        )
                        cur.execute(
                                format_log_sql
                            )
                        
                        for index,error_row in res.iterrows():
                            strKey = self.FormatKeyData(target_key, error_row)
                            self.FormatLogging('Error001', '更新対象が存在しません。', file_name, strKey, write_table=False)

                            #エラー対象のERROR_FLGを更新
                            error_key_row = controls[controls.search_key == True]
                            error_key_where = []
                            for error_idx,err_row in error_key_row.iterrows():
                                error_key_where.append(err_row['target_column'].lower()+'=\''+error_row[err_row['target_column'].lower()]+'\'')
                            error_key_where_join = ' AND '.join(error_key_where)

                            sql = 'UPDATE {} '\
                                'SET error_flg = TRUE '\
                                'WHERE {} '\
                                'AND PROCESS_DIV IS NOT NULL AND PROCESS_DIV <> \'\';'
                            cur.execute(
                                sql.format(
                                    work_dbname,
                                    error_key_where_join
                                )
                            )
                        
            
            
            #---------------------------------------------------------
            #各種件数の保持
            #--------------------------------------------------------- 
            self.total_num = self.GetWorkNum(work_dbname)
            self.error_num = self.GetWorkNumError(work_dbname)


            self.dwh_connection.commit()
        except Exception as e:
            self.dwh_connection.rollback()
            raise e

    ##########################################################
    #データ更新
    ##########################################################
    def RecData(self, controls, file_name):
        try:
            with self.dwh_connection.cursor() as cur:
                prd_db_name = ''
                wk_db_name = ''
                prd_tmp_name = ''

                update_set = []
                update_select = []
                update_where = ''

                insert_set = []
                insert_select = []
                insert_on = []
                
                
                delete_where = []

                laundering_set = []
                laundering_select = []

                for idx,row in controls.iterrows():
                    prd_db_name = row['target_schema']+'.'+row['target_table']
                    wk_db_name = row['target_work_schema']+'.'+row['target_work_table']
                    prd_tmp_name = row['target_schema']+'.TMP_'+row['target_table']

                    #更新先のカラム配列
                    update_set.append(row['target_column']+'= WK.'+row['target_column'])
                    insert_set.append(row['target_column'])
                    laundering_set.append(row['target_column'])

                    #更新キー
                    if row['search_key']:
                        update_where = update_where+' AND PRD.'+row['target_column']+'=WK.'+row['target_column']+' '
                        insert_on.append(' PRD.'+row['target_column']+'= WK.'+row['target_column'])
                        delete_where.append(' '+prd_db_name+'.'+row['target_column']+'= '+wk_db_name+'.'+row['target_column'])

                    #対象の型を取得
                    res = cur.execute(
                        'SELECT data_type,numeric_precision,numeric_scale FROM information_schema.columns WHERE table_schema = \'{}\' and table_name = \'{}\' and column_name = \'{}\';'.format(
                            row['target_schema'].lower(),
                            row['target_table'].lower(),
                            row['target_column'].lower()
                        )
                    )
                    cnt = cur.fetchone()
                    column_type = cnt[0]

                    # if row['target_column'].lower() == 'customer_id':
                    #     #顧客IDは起動時パラメータを使用する
                    #     update_select.append('\''+self.customer_id+'\' AS CUSTOMER_ID')
                    #     insert_select.append('\''+self.customer_id+'\' AS CUSTOMER_ID')
                    #     laundering_select.append('\''+self.customer_id+'\' AS CUSTOMER_ID')
                    if column_type.lower() in ['smallint', 'integer', 'bigint', 'int2', 'int4', 'int8']:
                        #整数型
                        update_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CASE WHEN NOT WK.'+row['target_column']+' ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN 0 ELSE cast(WK.'+row['target_column']+' as '+column_type.lower()+') END END')
                        insert_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CASE WHEN NOT WK.'+row['target_column']+' ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN 0 ELSE cast(WK.'+row['target_column']+' as '+column_type.lower()+') END END AS '+row['target_column'])
                        laundering_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CASE WHEN NOT WK.'+row['target_column']+'  ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN 0 ELSE cast(WK.'+row['target_column']+' as '+column_type.lower()+') END END AS '+row['target_column'])
                    elif column_type.lower() in ['real', 'double', 'preciision', 'double precision', 'float4', 'float8', 'float']:
                        #小数
                        update_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CASE WHEN NOT WK.'+row['target_column']+' ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN 0.0 ELSE cast(WK.'+row['target_column']+' as float) END END')
                        insert_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CASE WHEN NOT WK.'+row['target_column']+' ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN 0.0 ELSE cast(WK.'+row['target_column']+' as float) END END AS '+row['target_column'])
                        laundering_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CASE WHEN NOT WK.'+row['target_column']+' ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN 0.0 ELSE cast(WK.'+row['target_column']+' as float) END END AS '+row['target_column'])
                    elif column_type.lower() in ['decimal', 'numeric']:
                        #真数
                        #Redshift仕様によりDECIMAL(18,0)に18桁整数を入れようとするとオーバーフローになってしまうため特殊処理
                        if int(cnt[1]) == 18 and int(cnt[2]) == 0:
                            precision = int(cnt[1])+1
                            scale = int(cnt[2])
                        else :
                            precision = int(cnt[1])
                            scale = int(cnt[2])
                        update_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CASE WHEN WK.ERROR_FLG = TRUE THEN 0 WHEN NOT WK.'+row['target_column']+' ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN 0.0 ELSE cast(WK.'+row['target_column']+' as decimal('+str(precision)+','+str(scale)+')) END END')
                        insert_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CASE WHEN WK.ERROR_FLG = TRUE THEN 0  WHEN NOT WK.'+row['target_column']+' ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN 0.0 ELSE cast(WK.'+row['target_column']+' as decimal('+str(precision)+','+str(scale)+')) END END AS '+row['target_column'])
                        laundering_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CASE WHEN WK.ERROR_FLG = TRUE THEN 0  WHEN NOT WK.'+row['target_column']+' ~ \'^-?[0-9]+(\.[0-9]+)?$\' THEN 0.0 ELSE cast(WK.'+row['target_column']+' as decimal('+str(precision)+','+str(scale)+')) END END AS '+row['target_column'])
                    elif column_type.lower() in ['date']:
                        #日付
                        update_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CAST(WK.'+row['target_column']+' as date) END')
                        insert_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CAST(WK.'+row['target_column']+' as date) END AS '+row['target_column'])
                        laundering_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CAST(WK.'+row['target_column']+' as date) END AS '+row['target_column'])
                    elif column_type.lower() in ['time', 'timetz', 'time without time zone', 'time with time zone', 'timestamp without time zone', 'timestamp with time zone']:
                        #日時
                        update_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CAST(WK.'+row['target_column']+' as '+column_type.lower()+') END')
                        insert_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CAST(WK.'+row['target_column']+' as '+column_type.lower()+') END AS '+row['target_column'])
                        laundering_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN null ELSE CAST(WK.'+row['target_column']+' as '+column_type.lower()+') END AS '+row['target_column'])
                    elif column_type.lower() in ['char', 'verchar', 'character', 'nchar', 'bpchar', 'character varying', 'nvarchar', 'text']:
                        #文字列
                        #改行の置き換え
                        update_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN NULL ELSE REPLACE(  WK.'+row['target_column']+', \'\\n\' , \'\\r\\n\') END AS '+row['target_column'])
                        insert_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN NULL ELSE REPLACE(  WK.'+row['target_column']+', \'\\n\' , \'\\r\\n\') END AS '+row['target_column'])
                        laundering_select.append('CASE WHEN WK.'+row['target_column']+' IS NULL OR WK.'+row['target_column']+' = \'\' THEN NULL ELSE REPLACE(  WK.'+row['target_column']+', \'\\n\' , \'\\r\\n\') END AS '+row['target_column'])
                    elif column_type.lower() in ['boolean', 'bool']:
                        #BOOL
                        update_select.append('CASE WHEN WK.'+row['target_column']+' IN (\'TRUE\',\'True\',\'true\') THEN TRUE WHEN WK.'+row['target_column']+' IN (\'FALSE\',\'False\',\'false\') THEN FALSE ELSE NULL END AS '+row['target_column'])
                        insert_select.append('CASE WHEN WK.'+row['target_column']+' IN (\'TRUE\',\'True\',\'true\') THEN TRUE WHEN WK.'+row['target_column']+' IN (\'FALSE\',\'False\',\'false\') THEN FALSE ELSE NULL END AS '+row['target_column'])
                        laundering_select.append('CASE WHEN WK.'+row['target_column']+' IN (\'TRUE\',\'True\',\'true\') THEN TRUE WHEN WK.'+row['target_column']+' IN (\'FALSE\',\'False\',\'false\') THEN FALSE ELSE NULL END AS '+row['target_column'])
                    else:
                        update_select.append('WK.'+row['target_column'])
                        insert_select.append('WK.'+row['target_column'])
                        laundering_select.append('WK.'+row['target_column'])
                
                

                #---------------------------------------------------------
                #Upsert
                #---------------------------------------------------------
                
                update_num = self.GetProcessDivNum(controls, 'U')
                if self.IS_DEBUG:
                        self.LoggingWithTime('Process division U:'+str(update_num), 'info')

                if not update_num == 0:
                    if self.IS_DEBUG:
                        self.LoggingWithTime('Start Upsert process', 'info')

                    #更新先のカラムにシステム項目を追加
                    update_set.append('LAST_UPD_ID = WK.UPD_LAST_UPD_ID')
                    update_set.append('LAST_UPD_DTT = WK.UPD_LAST_UPD_DTT')
                    update_set_join = ", ".join(update_set)

                    update_select.append('WK.ERROR_FLG')
                    update_select.append('\'ETL\' AS UPD_LAST_UPD_ID')
                    update_select.append(self.SQL_NOW+' AS UPD_LAST_UPD_DTT')
                    update_select_join = ", ".join(update_select)

                    #UPDATE
                    if self.IS_DEBUG:
                        self.LoggingWithTime('Start Update process', 'info')

                    sql = 'UPDATE {} AS PRD SET {} '\
                        + 'FROM (SELECT {} FROM {} AS WK WHERE WK.PROCESS_DIV = \'U\') AS WK '\
                        + 'WHERE WK.ERROR_FLG = False {};'
                    sql = sql.format(
                        prd_db_name,
                        update_set_join,
                        update_select_join,
                        wk_db_name,
                        update_where
                    )
                    cur.execute(
                        sql
                    )

                    #INSERTするシステム項目を設定
                    insert_set.append('CREATE_ID')
                    insert_set.append('CREATE_DTT')
                    insert_set.append('LAST_UPD_ID')
                    insert_set.append('LAST_UPD_DTT')
                    insert_set_join = ', '.join(insert_set)

                    insert_select.append('\'ETL\' AS CREATE_ID')
                    insert_select.append(self.SQL_NOW+' AS CREATE_DTT')
                    insert_select.append('\'ETL\' AS LAST_UPD_ID')
                    insert_select.append(self.SQL_NOW+' AS LAST_UPD_DTT')
                    insert_select_join = ', '.join(insert_select)
                    insert_on_join = ' AND '.join(insert_on)

                    if self.IS_DEBUG:
                        self.LoggingWithTime('Start Insert process', 'info')

                    #INSERT
                    sql = 'INSERT INTO {} ({}) '\
                        + 'SELECT {} FROM {} AS WK '\
                        + 'LEFT JOIN {} as PRD on {} '\
                        + 'WHERE PRD.CREATE_ID IS NULL AND PROCESS_DIV=\'U\' AND WK.ERROR_FLG = False;'
                    sql = sql.format(
                        prd_db_name,
                        insert_set_join,
                        insert_select_join,
                        wk_db_name,
                        prd_db_name,
                        insert_on_join
                    )
                    cur.execute(
                        sql
                    )
                    
                    self.is_upsert = True
                    self.success_num = self.success_num + update_num

                    if self.IS_DEBUG:
                        self.LoggingWithTime('Finish Upsert process', 'info')

                #---------------------------------------------------------
                #Delete
                #---------------------------------------------------------
                delete_num = self.GetProcessDivNum(controls, 'D')

                if self.IS_DEBUG:
                        self.LoggingWithTime('Process division D:'+str(delete_num), 'info')

                if not delete_num == 0:
                    delete_where_join = ' AND '.join(delete_where)
                    #DELETE

                    if self.IS_DEBUG:
                        self.LoggingWithTime('Start delete process', 'info')

                    sql = 'DELETE FROM {} '\
                        + 'USING {} '\
                        + 'WHERE {}.ERROR_FLG=False AND {}.PROCESS_DIV=\'D\' AND {};'
                    sql = sql.format(
                        prd_db_name,
                        wk_db_name,
                        wk_db_name,
                        wk_db_name,
                        delete_where_join
                    )
                    cur.execute(
                        sql
                    )

                    self.is_delete = True
                    self.success_num = self.success_num + delete_num

                    if self.IS_DEBUG:
                        self.LoggingWithTime('Finish delete process', 'info')

                #---------------------------------------------------------
                #Laundering
                #---------------------------------------------------------
                laundering_num = self.GetProcessDivNum(controls, 'L')
                
                if self.IS_DEBUG:
                        self.LoggingWithTime('Process division L:'+str(laundering_num), 'info')

                if not laundering_num == 0:
                    if self.IS_DEBUG:
                        self.LoggingWithTime('Start laundering process', 'info')

                    #テーブル定義をコピーしてテーブルを作成
                    if self.IS_DEBUG:
                        self.LoggingWithTime('Start create table', 'info')
                    sql = 'CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS);'
                    sql = sql.format(
                        prd_tmp_name,
                        prd_db_name
                    )
                    cur.execute(
                        sql
                    )

                    if self.IS_DEBUG:
                        self.LoggingWithTime('Finish create table', 'info')


                    #データをインサート
                    #INSERTするシステム項目を設定
                    laundering_set.append('CREATE_ID')
                    laundering_set.append('CREATE_DTT')
                    laundering_set.append('LAST_UPD_ID')
                    laundering_set.append('LAST_UPD_DTT')
                    laundering_set = ', '.join(laundering_set)

                    laundering_select.append('\'ETL\' AS CREATE_ID')
                    laundering_select.append(self.SQL_NOW+' AS CREATE_DTT')
                    laundering_select.append('\'ETL\' AS LAST_UPD_ID')
                    laundering_select.append(self.SQL_NOW+' AS LAST_UPD_DTT')
                    laundering_select_join = ', '.join(laundering_select)

                    if self.IS_DEBUG:
                        self.LoggingWithTime('Start insert to new table', 'info')
                    
                    #INSERT
                    sql = 'INSERT INTO {} ({}) '\
                        + 'SELECT {} FROM {} AS WK '\
                        + 'WHERE WK.PROCESS_DIV=\'L\' AND WK.ERROR_FLG = False;'
                    sql = sql.format(
                        prd_tmp_name,
                        laundering_set,
                        laundering_select_join,
                        wk_db_name,
                    )
                    cur.execute(
                        sql
                    )

                    if self.IS_DEBUG:
                        self.LoggingWithTime('Finish insert to new table', 'info')

                    if self.IS_DEBUG:
                        self.LoggingWithTime('Rename table prd to bk', 'info')

                    #テーブルをリネーム
                    sql = 'ALTER TABLE {} RENAME TO {}_BK;'
                    sql = sql.format(
                        prd_db_name,
                        self.target_table
                    )
                    cur.execute(
                        sql
                    )

                    if self.IS_DEBUG:
                        self.LoggingWithTime('Rename table new table to prd', 'info')

                    sql = 'ALTER TABLE {} RENAME TO {};'
                    sql = sql.format(
                        prd_tmp_name,
                        self.target_table
                    )
                    cur.execute(
                        sql
                    )

                    if self.IS_DEBUG:
                        self.LoggingWithTime('Drop table bk', 'info')

                    #テーブル削除
                    sql = 'DROP TABLE {}_BK;'
                    sql = sql.format(
                        prd_db_name
                    )
                    cur.execute(
                        sql
                    )
                    self.success_num = self.success_num + self.GetWorkNumError(wk_db_name, False)

            self.dwh_connection.commit()

            return True
        except Exception as e:
            self.dwh_connection.rollback()
            raise e


        return True
    

def main():
    main_flow = MainFlow()

    #DWHへのコネクション確認
    chk_dwh = main_flow.ChkDwhConnection()

    #処理開始
    main_flow.FormatLogging('Info001', 'バッチ処理を開始します。')

    #処理開始メール送信
    main_flow.SendJobStartMail()
    main_flow.JOB_STATUS = True

    #---------------------------------------------------------
    #処理対象テーブル一覧の取得
    #---------------------------------------------------------
    tables = main_flow.TargetTable()

    #ファイルが取得できた場合
    if not tables.empty and chk_dwh:
        #---------------------------------------------------------
        #ファイル処理ループ
        #---------------------------------------------------------
        try:
            for idx,table in tables.iterrows():
                #エラーメール送信フラグのリセット
                main_flow.is_sent_error_mail = False
                main_flow.is_sent_fatal_mail = False

                #ファイルコードのリセット
                main_flow.file_code = table['aurora_table']

                #各種テーブルのリセット
                main_flow.target_work_schema = ''
                main_flow.target_work_table = ''
                main_flow.target_schema = ''
                main_flow.target_table = ''

                #エラーフラグのリセット
                main_flow.is_error = False

                #各種処理件数カウント
                main_flow.total_num = 0
                main_flow.success_num = 0
                main_flow.error_num = 0

                
                #Info004ログ出力用
                is_insert = False
                is_upsert = False
                is_delete = False

                main_flow.FormatLogging('Info003', '対象テーブルの取り込みを開始します。', table['aurora_table'])

                #---------------------------------------------------------
                #制御マスタチェック
                #---------------------------------------------------------
                if main_flow.IS_DEBUG:
                        main_flow.Logging('制御マスタチェック 開始', 'info')

                res = main_flow.ValidMstControl(table['aurora_table'])

                #制御マスタチェックでのエラーはFatalの為処理中断
                if not res:
                    if main_flow.IS_DEBUG:
                        main_flow.Logging('制御マスタチェック エラー', 'info')

                    #異常終了通知
                    main_flow.JOB_STATUS=False

                    break

                if main_flow.IS_DEBUG:
                        main_flow.Logging('制御マスタチェック 完了', 'info')
                
                #制御マスタのデータを取得
                controls = main_flow.GetMstControl(table['aurora_table'])

                #---------------------------------------------------------
                #ワークテーブル取り込み
                #---------------------------------------------------------
                try:
                    if main_flow.IS_DEBUG:
                        main_flow.Logging('ワークテーブル取り込み 開始', 'info')

                    main_flow.ImportWorkTable(controls,table['aurora_table'])

                    if main_flow.IS_DEBUG:
                        main_flow.Logging('ワークテーブル取り込み 完了', 'info')

                except Exception as e:
                    if main_flow.IS_DEBUG:
                        main_flow.Logging('ワークテーブル取り込み エラー', 'info')

                    if str(e) == 'Info010':
                        main_flow.FormatLogging('Info010', '処理対象データが存在しないため処理をスキップします。', table['aurora_table'])
                        continue

                    #異常終了通知
                    main_flow.JOB_STATUS=False
                    
                    if str(e) == 'Fatal007':
                        main_flow.FormatLogging('Info006', 'データの取り込みを完了します。', table['aurora_table'])
                        continue
                    else:
                        raise e

                    

                #---------------------------------------------------------
                #妥当性チェック
                #---------------------------------------------------------
                try:
                    if main_flow.IS_DEBUG:
                        main_flow.Logging('妥当性チェック 開始', 'info')

                    main_flow.ValidData(controls, table['aurora_table'])

                    if main_flow.IS_DEBUG:
                        main_flow.Logging('妥当性チェック 完了', 'info')

                except Exception as e:
                    #異常終了通知
                    main_flow.JOB_STATUS=False
                    if main_flow.IS_DEBUG:
                        main_flow.Logging('妥当性チェック エラー', 'info')

                    if main_flow.IS_DEBUG:
                        main_flow.Logging('妥当性チェック エラーが発生しました。【'+str(e)+'】', 'info')

                    # 顧客IDチェックエラー出力
                    if str(e) == 'Fatal003':
                        #ファイルのコピーをerrorに移動して中断
                        main_flow.FormatLogging('Info006', '対象テーブルの取り込みを完了します。', table['aurora_table'])
                        continue
                    # 処理区分チェックもファイルスキップ
                    elif str(e) == 'Fatal014' or str(e) == 'Fatal015' or str(e) == 'Fatal018'or str(e) == 'Fatal019':
                        #ファイルのコピーをerrorに移動して中断
                        main_flow.FormatLogging('Info006', '対象テーブルの取り込みを完了します。', table['aurora_table'])
                        continue
                    #それ以外の例外は異常終了
                    elif 'Fatal0' in str(e):
                        break
                    else:
                        raise e

                #---------------------------------------------------------
                #データ更新
                #---------------------------------------------------------
                try:
                    if main_flow.IS_DEBUG:
                        main_flow.Logging('データ更新 開始', 'info')

                    main_flow.RecData(controls, table['aurora_table'])

                    if main_flow.IS_DEBUG:
                        main_flow.Logging('データ更新 完了', 'info')

                except Exception as e:
                    if main_flow.IS_DEBUG:
                        main_flow.Logging('データ更新 エラー', 'info')

                    #異常終了通知
                    main_flow.JOB_STATUS=False    
                    raise e
                
                #---------------------------------------------------------
                #エラー行の処理
                #---------------------------------------------------------
                error_num = main_flow.GetWorkNumError(main_flow.target_work_schema+'.'+main_flow.target_work_table)

                if main_flow.IS_DEBUG:
                        main_flow.Logging('エラー件数:'+str(error_num), 'info')

                info004_text = main_flow.GenerateInfo004(is_insert, is_upsert, is_delete)
                main_flow.FormatLogging('Info004', main_flow.target_table+'で'+str(main_flow.success_num)+'件のデータを'+info004_text+'しました。', table['aurora_table'])
                main_flow.FormatLogging('Info005', '処理合計件数:'+str(main_flow.total_num)+'件、正常終了:'+str(main_flow.success_num)+'件、エラー:'+str(main_flow.error_num)+'件。', table['aurora_table'], total_num = main_flow.total_num, normal_num = main_flow.success_num, error_num = main_flow.error_num )
                main_flow.FormatLogging('Info006', 'データの取り込みを完了します。', table['aurora_table'])

        except Exception as e:
            #Fatalで終了した場合は処理終了
            main_flow.FormatLogging('Fatal008', ' DBでの処理時にエラーが発生しました。【'+str(e)+'】', main_flow.file_code, 0)
            print(traceback.format_exc())

            #異常終了通知
            main_flow.JOB_STATUS=False

    #対象テーブルがない場合はログを出力して終了
    elif tables.empty and chk_dwh:
        main_flow.FormatLogging('Info007', '処理対象テーブルが存在しないためバッチ処理を終了します。')
    

    #処理終了
    main_flow.FormatLogging('Info002', 'バッチ処理を終了します。')


    #DWHのコネクションの破棄
    if main_flow.dwh_connection:
        main_flow.dwh_connection.close()

    #処理終了メール送信
    main_flow.SendJobFinishMail(main_flow.JOB_STATUS)
 
    #CloudWatchへの連続ログ出力の不具合対策
    #10秒間のスリーブ
    time.sleep(10)
    
    main_flow.JobCommit()
    main_flow.TriggerPublish()
    

if __name__ == '__main__':
    main()
