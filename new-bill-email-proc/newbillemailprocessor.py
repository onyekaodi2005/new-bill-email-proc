import time
import pytz
import argparse
import traceback
import asyncio
import aiohttp
import socket
import copy
import sys
import json
import os
from tqdm import tqdm, trange
from ppxbepdao import bepdaofactory
from datetime import datetime, timedelta
from email_validator import validate_email, EmailNotValidError
from ppxbepcommon.workers.insertauditlog import insertauditlog
from ppxbepcommon.constants.functionalcontrols import FxControl, Error
from ppxbepcommon.workers.getaposcheduledpayments import getscheduledpayments
from ppxbepcommon.workers.getactiveinvoices import getActiveInvoices
# from ppxbepcommon.workers.updatebepstatus import updateBEPStatus
from ppxbepcommon.utils.ppxprocessor import ppxprocessor, logIt, createPropertiesfile
from ppxbepcommon.constants.constantMethods import (
    seconds_to_time_format, getControlCode, select_values)
from ppxbepcommon.workers.getinboundfilesize import getinboundfilesize
from ppxbepcommon.workers.callemailwrapperservice import callEmailWrapperService


class newbillemailprocessor(ppxprocessor):

    def __init__(self):
        pass

    async def run(self, **kwargs):
        try:
            self.bepenv, rundate, jobdays = kwargs.get(
                'bepenv'), kwargs.get('rundate'), kwargs.get('jobdays')
            batchQueryCall = int(child.parser.get(
                'procProperties', 'BATCH_QUERY_CALL', fallback=3))
            self.semaphore = asyncio.BoundedSemaphore(batchQueryCall)
            startTime = datetime.today().timestamp()
            self.processordoc = {'failures': [],
                                 'warnings': [],
                                 'mongoDbList': {},
                                 'scheduledpayment': []
                                 }
            self.auditdoc = {
                "argumentsCML": kwargs,
                "touchpointId": touchPointId,
                "processName": child.parser.get('procProperties', "PROCESS_NAME"),
                "batchName": kwargs.get('batchname'),
                "originationName": child.parser.get('procProperties', "ORIGINATION_NAME", fallback='PPX').upper(),
                "destinationName": child.parser.get('procProperties', "DESTINATION_NAME", fallback='PPX').upper(),
                "MCOWS": [],
                "startTime": startTime,
                "status": "SUCCESS",
                "recordsCount": {
                    "totalRecords": 0,
                    "failureRecords": 0,
                    "successRecords": 0,
                    "warningRecords": 0
                },
                "fileNames": [],
                "runtime": None
            }
            self.statusdoc = {"sysStatus": False}

            # check for rundate and derive fromDt and toDt
            if rundate:
                toDt = datetime.strptime(str(rundate), '%Y%m%d')
            elif child.parser.has_option('inputParameters', "RUNDATE"):
                if child.parser.get('inputParameters', "RUNDATE") != "None" and len(child.parser.get('inputParameters', "RUNDATE")) == 8:
                    toDt = datetime.strptime(child.parser.get(
                        'inputParameters', "RUNDATE"), '%Y%m%d')
                elif child.parser.get('inputParameters', "RUNDATE") == "None":
                    toDt = datetime.today()
                elif len(child.parser.get('inputParameters', "RUNDATE")) != 8:
                    self.auditdoc['status'] = 'ERROR'
                    self.auditdoc.update({"PPXControlCode": Error['Format']['errorCode'],
                                          'errorDetails': 'Invalid Rundate Format Provided From Config - Should be YYYYMMDD'})
                    logIt(self, message='Invalid Rundate Format Provided From Config - Should be (YYYYMMDD)',
                          ppx_controlcode=Error['Format']['errorCode'], logAs='exception')
                    return await self.insertauditdoc()
            else:
                toDt = datetime.today()

            toDt = toDt.replace(minute=int(child.parser.get('procProperties', "JOB_TODT_MINUTES")), hour=int(
                child.parser.get('procProperties', "JOB_TODT_HOURS")), second=59, microsecond=999999)

            tz = pytz.timezone('America/New_York')
            toDt = datetime.fromtimestamp(toDt.timestamp(), tz)

            # JOBDAYS: For the range of days to run the processor
            if jobdays:
                fromDt = toDt - timedelta(days=int(jobdays))
            elif child.parser.has_option('inputParameters', "JOBDAYS") and child.parser.get('inputParameters', "JOBDAYS") != "None":
                fromDt = toDt - \
                    timedelta(days=int(child.parser.get(
                        'inputParameters', "JOBDAYS")))
            else:
                fromDt = toDt - timedelta(days=1)

            # toDt = toDt.timestamp()
            fromDt = datetime.fromtimestamp(fromDt.timestamp(), tz)
            fromDt = fromDt.replace(minute=int(child.parser.get('procProperties', 'JOB_FROMDT_MINUTES')), hour=int(
                child.parser.get('procProperties', "JOB_FROMDT_HOURS")), second=0, microsecond=0).timestamp()
            fromDt = datetime.fromtimestamp(fromDt, tz)

            ############################ GET COUNT OF ACTIVE INVOICE ####################################
            startTime = time.time()
            pdoc = {'source': child.parser.get('procProperties', 'ENROLLMENTSOURCESYSTEM_LIST').replace(' ', '').split(','),
                    'count': True, 'fromDt': fromDt, 'toDt': toDt, 'newBill': True}
            logIt(self, message='Arguments passed to getactiveinvoices mcow count',
                  arguments=json.dumps(pdoc, default=str), logAs='info')
            for _ in trange(1, desc='GET ACTIVE INVOICES COUNT LOADING...'):
                result = await getActiveInvoices(child.parser).doWork(pdoc, child.dbinstances['mongoDB'])
            logIt(self, message='response from getactiveinvoices mcow count: ' +
                  str(result), logAs='debug')

            self.auditdoc["MCOWS"].append({
                "name": "getactiveinvoices_count",
                "startTime": startTime,
                "count": result.get('count'),
                "endTime": time.time(),
                "runtime": seconds_to_time_format(time.time() - startTime),
                "error": result.get('error', ''),
                "errorMessage": result.get('errorMessage', '')
            })

            if result['-retCode'] == 9999:
                errorMessage = result.get(
                    'errorMessage', 'ERROR OCCURRED IN GETACTINVOICES MCOW COUNT')
                self.auditdoc.update({"PPXControlCode": getControlCode(
                    result), 'status': 'ERROR', 'errorMessage': errorMessage})
                logIt(self, message=result.get('error', 'error occured in getnascopayment mcow count'),
                      ppx_controlcode=FxControl['FieldMissing']['ControlCode'], logAs='exception')
                await self.insertauditdoc()
            logIt(
                self, message='Completed Call to getactiveinvoices mcow count', logAs='info')

            self.auditdoc["recordsCount"]["totalRecords"] = result.get('count')
            if result['count'] == 0:
                logIt(self, message='No Records Found After Processing',
                      ppx_controlcode=FxControl['NoTxProcessed']['ControlCode'], logAs='info')
                self.auditdoc.update(
                    {"PPXControlCode": FxControl['NoTxProcessed']['ControlCode']})
                await self.insertauditdoc()

            ############################ GET READ OF ACTIVE INVOICE ####################################
            limit, recordCount, getActive_read = int(child.parser.get(
                'procProperties', "DB_BATCH_SIZE", fallback=1000)), result['count'], []
            del pdoc['count']

            for offset in range(0, recordCount, limit):
                newDoc = copy.deepcopy(pdoc)
                newDoc.update({'skip': offset, 'limit': limit})
                getActive_read.append(self.get_active_invoice_read(newDoc))

            self.loading = trange(
                len(getActive_read), desc=f"GET ACTIVE INVOICE MCOW ({recordCount}) LOADING...")
            tasks_complete, tasks_incomplete = await asyncio.wait(getActive_read)
            self.loading.close()

            if len(tasks_incomplete) > 0:
                self.processordoc['failures'].append(tasks_incomplete)

            for rez in tasks_complete:
                rez = rez.result()
                for i in rez['data']['payments']:
                    if i['source'] in self.processordoc['mongoDbList']:
                        self.processordoc['mongoDbList'][i['source']].append(i)
                    else:
                        self.processordoc['mongoDbList'][i['source']] = [i]

            mongoList = {}
            for k, v in self.processordoc['mongoDbList'].items():
                self.auditdoc["recordsCount"].update(
                    {f'{k}_total_from_mongoDB': len(v)})
                result = await self.validate_mongo_db_rec(v, **{'enrollment': k})
                p = list(result.keys())
                if p[0] in mongoList:
                    mongoList[p[0]].extend(result[p[0]])
                else:
                    mongoList.update(result)
                self.auditdoc["recordsCount"].update(
                    {f'{k}_success_from_mongoDB': len(mongoList[p[0]])})

            self.processordoc['mongoDbList'] = mongoList
            getScheduledList = []

            select = ""
            if child.parser.get('procProperties', 'NEW_BILL_SELECT_LIST') and child.parser.get('procProperties', 'NEW_BILL_SELECT_LIST') != "None" and child.parser.get('procProperties', 'NEW_BILL_SELECT_LIST').strip():
                alias = child.parser.get(
                    'procProperties', 'NEW_BILL_SELECT_ALIAS', fallback="{}")
                selectDoc = {'select_value': child.parser.get(
                    'procProperties', 'NEW_BILL_SELECT_LIST'), 'select_alias': alias, 'char_replace': "'"}
                select = select_values(**selectDoc)

            limit = int(child.parser.get('procProperties',
                                         "DB_BATCH_SIZE", fallback=1000))
            for k, v in self.processordoc['mongoDbList'].items():
                if not v:
                    continue

                v = [i.get('contractId') for i in v]
                recordCount = len(v)
                for offset in range(0, recordCount, limit):
                    pdoc = {'enrollmentsourcesystem': k, 'contractid': v[offset: offset+limit], 'apoindicator': child.parser.get(
                        'procProperties', 'APO_INDICATOR', fallback='Y'), 'select': select}
                    getScheduledList.append(self.get_scheduled_payments(
                        pdoc, **{'limit': offset, 'offset': offset+limit}))

            if getScheduledList:
                self.loading = trange(
                    len(getScheduledList), desc=f"GET SCHEDULED PAYMENT MCOW LOADING...")
                tasks_complete, tasks_incomplete = await asyncio.wait(getScheduledList)
                self.loading.close()

                if tasks_incomplete:
                    self.processordoc['warnings'].append(tasks_incomplete)

                for i in tasks_complete:
                    i = i.result()
                    if i.get('data'):
                        self.processordoc['scheduledpayment'].extend(
                            i['data']['scheduledPayments'])
                self.auditdoc["recordsCount"].update(
                    {'total_from_scheduledPayments': len(self.processordoc['scheduledpayment'])})

                ########################################## VALIDATE MONGODB AGAINST SCHEDULED PAYMENTS AND VALIDATE SCHEDULED PAYMENTS ############################################
                kwargs = {
                    '_REQ_FIELD': child.parser.get('procProperties', 'SCHEDULED_PAYMENT_API_REQ_FIELDS_LIST', fallback='').replace(' ', '').split(','),
                    'RBMS_REQ_FIELD': child.parser.get('procProperties', 'RBMS_SCHEDULED_PAYMENT_API_REQ_FIELDS_LIST', fallback='').replace(' ', '').split(','),
                    'SAPHR_REQ_FIELD': child.parser.get('procProperties', 'SAPHR_SCHEDULED_PAYMENT_API_REQ_FIELDS_LIST', fallback='').replace(' ', '').split(','),
                    'NASCO_REQ_FIELD': child.parser.get('procProperties', 'NASCO_SCHEDULED_PAYMENT_API_REQ_FIELDS_LIST', fallback='').replace(' ', '').split(','),
                    'RBMS_ALIAS': await self.validate_alias_json(child.parser.get('procProperties', 'RBMS_API_CALL_ALIAS_JSON', fallback={})),
                    'SAPHR_ALIAS': await self.validate_alias_json(child.parser.get('procProperties', 'SAPHR_API_CALL_ALIAS_JSON', fallback={})),
                    'NASCO_ALIAS': await self.validate_alias_json(child.parser.get('procProperties', 'NASCO_API_CALL_ALIAS_JSON', fallback={})),
                    'emailField': child.parser.get('procProperties', 'SCHEDULED_PAYMENT_EMAIL_FIELDS_LIST', fallback='').replace(' ', '').split(','),
                    'numericField': child.parser.get('procProperties', 'SCHEDULED_PAYMENT_EXCLUDE_NULL_FIELDS_LIST', fallback='').replace(' ', '').split(','),
                    'excludeNull': child.parser.get('procProperties', 'SCHEDULED_PAYMENT_EXCLUDE_NULL_FIELDS_LIST', fallback='').replace(' ', '').split(','),
                }

                validContract = [i['contractId']
                                 for i in self.processordoc['scheduledpayment']]
                newAPIData = []
                for k, v in self.processordoc['mongoDbList'].items():
                    for i in v:
                        if i['contractId'] not in validContract:
                            i.update(
                                {'errorMessage': 'RECORD NOT FOUND IN SCHEDULED PAYMENTS TABLE', 'recordType': 'WARNING'})
                            self.processordoc['warnings'].append(i)
                        else:
                            keyToDel, newDic, recsToUpdate = [], {}, []
                            for ke, va in i.items():
                                if isinstance(va, dict):
                                    newDic = {}
                                    for key, val in va.items():
                                        newDic.update({key: val})
                                    if ke not in kwargs['_REQ_FIELD']:
                                        keyToDel.append(ke)
                                if newDic:
                                    recsToUpdate.append(newDic)
                                    newDic = {}
                            for m in keyToDel:
                                del i[m]
                            for m in recsToUpdate:
                                i.update(m)
                            newAPIData.append(i)
                self.processordoc['mongoDbList'] = newAPIData
                for m in self.processordoc['scheduledpayment']:
                    for i in self.processordoc['mongoDbList']:
                        if m['contractId'] == i['contractId']:
                            m.update(i)

                result = await self.validate_scheduled_payments(self.processordoc['scheduledpayment'], **kwargs)
                self.auditdoc["recordsCount"].update(
                    {'success_from_scheduledPayments': len(result)})

                self.processordoc['scheduledpayment'] = result

            if self.processordoc['scheduledpayment']:
                optionalFields = child.parser.get(
                    'procProperties', 'EMAIL_WRAPPER_OPT_FIELDS', fallback='{}').replace("'", '"')
                requiredFields = child.parser.get(
                    'procProperties', 'EMAIL_WRAPPER_REQ_FIELDS', fallback='{}').replace("'", '"')
                api_call_list = []
                for data in self.processordoc['scheduledpayment'][0:2]:
                    pdoc = {'newBillEmail':
                            {'optionalFields': json.loads(optionalFields),
                             'requiredFields': json.loads(requiredFields)},
                            }
                    pdoc.update(data)
                    api_call_list.append(self.call_email_wrapper_api(pdoc))
                ################################## BATCH CALLS TO API ##########################################
                batchQueryCall = int(child.parser.get(
                    'procProperties', 'BATCH_API_CALL', fallback=3))
                self.semaphore = asyncio.BoundedSemaphore(batchQueryCall)
                startTime = time.time()
                self.loading = trange(len(
                    api_call_list), desc=f"CALL TO EMAIL WRAPPER API ({len(api_call_list)}) LOADING...")
                tasks_complete, tasks_incomplete = await asyncio.wait(api_call_list)

                if tasks_incomplete:
                    self.processordoc['failures'].append(tasks_incomplete)

                success = failure = 0
                for rez in tasks_complete:
                    rez = rez.result()
                    if rez.get('-retCode') != 0:
                        self.auditdoc["recordsCount"]['failureRecords'] += 1
                        rez.update({'recordType': 'FAILURE'})
                        self.processordoc['failures'].append(rez)
                        failure += 1
                    else:
                        self.auditdoc["recordsCount"]['successRecords'] += 1
                        success += 1

                self.auditdoc["MCOWS"].append({
                    "name": "callemailwrapperservice_mcow",
                    "startTime": startTime,
                    "endTime": time.time(),
                    "runtime": seconds_to_time_format(time.time() - startTime),
                    "SUCCESS": success,
                    "FAILURE": failure
                })

            if self.auditdoc["recordsCount"]['successRecords'] > 0 and self.auditdoc["recordsCount"]['failureRecords'] > 0:
                logIt(self, message='Some Successful and Error Dishonor TurnOff Generated: ',
                      ppx_controlcode=FxControl['SomePaymentsSuccessful']['ControlCode'], logAs='info')
                self.auditdoc.update(
                    {"PPXControlCode": FxControl['SomePaymentsSuccessful']['ControlCode'], "status": "PARTIAL-SUCCESS"})
            elif self.auditdoc["recordsCount"]['successRecords'] > 0 and self.auditdoc["recordsCount"]['failureRecords'] == 0:
                logIt(self, message='All Payments were successful: ',
                      ppx_controlcode=FxControl['AllPaymentsSuccessful']['ControlCode'], logAs='info')
                self.auditdoc.update(
                    {"PPXControlCode": FxControl['AllPaymentsSuccessful']['ControlCode'], "status": "SUCCESS"})
            elif self.auditdoc["recordsCount"]['successRecords'] == 0 and self.auditdoc["recordsCount"]['failureRecords'] != 0:
                logIt(self, message='No records to be processed: ',
                      ppx_controlcode=FxControl['NoTxBatch']['ControlCode'], logAs='info')
                self.auditdoc.update(
                    {"PPXControlCode": FxControl['NoTxBatch']['ControlCode'], "status": "FAILURE", "sysStatus": True})
            elif self.auditdoc["recordsCount"]['successRecords'] == 0 and self.auditdoc["recordsCount"]['failureRecords'] == 0:
                logIt(self, message='No records to be processed: ',
                      ppx_controlcode=FxControl['NoTxProcessed']['ControlCode'], logAs='info')
                self.auditdoc.update(
                    {"PPXControlCode": FxControl['NoTxProcessed']['ControlCode'], "status": "SUCCESS"})

            await self.insertauditdoc()

        except Exception as ex:  # pragma: no cover
            print(traceback.print_exc())
            logIt(self, errorMessage=str(ex),
                  logAs='exception')  # pragma: no cover
            self.auditdoc.update({
                'status': 'ERROR',
                'errorMessage': f"""EXCEPTION OCCURRED WHILE PROCESSING - {str(ex).replace("'",'')}""",
                'PPXControlCode': Error['Exception']['errorCode']})
            return await self.insertauditdoc()

    async def validate_alias_json(self, dataString, char_replace="*"):
        try:
            dataString = dataString.replace(
                char_replace, '"').replace("'", '"')
            return json.loads(dataString)
        except Exception as ex:
            print(f'UNABLE TO TRANSFORM ALIAS JSON - {str(ex)}')
            return {}

    async def validate_scheduled_payments(self, pdocList, **kwargs):
        requiredFields = []
        emailField = kwargs['emailField']
        numericField = kwargs['numericField']
        excludeNull = kwargs['excludeNull']
        recordList = []
        for data in pdocList:
            # data.update({'receiptEmailAddress':'Onyeka.Odi@bcbsfl.com', 'firstNm': 'Odi, Onyeka'})
            invalid, record = False, {}
            if data.get('source'):
                requiredFields = kwargs.get(
                    f"{str(data['source']).replace(' ','').upper()}_REQ_FIELD")
                alias_json = kwargs.get(
                    f"{str(data['source']).replace(' ','').upper()}_ALIAS")
                if not requiredFields:
                    requiredFields = []
                if not alias_json:
                    alias_json = {}
            else:
                requiredFields = []
            for i in requiredFields:
                new_name = alias_json.get(i)
                record.update({new_name if new_name else i: data.get(i)})
                if not data.get(i) or i not in data or not str(data[i]).strip():
                    if i not in excludeNull:
                        otherReq = requiredFields
                        otherReq.pop(otherReq.index(i))
                        message = f"""MISSING OR EMPTY REQUIRED FIELD ({i}) - OTHER REQ FIELD ({str(tuple(otherReq)).replace("'","")})"""
                        data.update({'errorMessage': message})
                        invalid = True
                        break
                    else:
                        del record[new_name if new_name else i]
                if i in emailField:
                    try:
                        valid = validate_email(str(data.get(i)))
                        del valid
                    except EmailNotValidError as ex:
                        data.update(
                            {'errorMessage': f"""INVALID OR MISSING EMAIL FIELD ({i}) - {str(ex)}"""})
                        invalid = True
                        break
                if i in numericField and not str(data.get(i)).replace('.', '').isnumeric():
                    data.update(
                        {'errorMessage': f"""INVALID OR MISSING NUMERIC FIELD ({i})"""})
                    invalid = True
                    break
            if not invalid and record:
                if not record.get('languageCode'):
                    record.update({'languageCode': child.parser.get(
                        'procProperties', 'DEFAULT_LANGUAGE_CODE', fallback='en')})
                ppxinternal = child.parser.get(
                    'procProperties', 'PPX_INTERNAL_REQUEST', fallback='True').lower()
                try:
                    ppxinternal = ppxinternal[0].upper(
                    )+ppxinternal[1:].lower()
                except:
                    ppxinternal = 'True'
                record.update({'ppxInternalRequest': eval(ppxinternal),
                               "templateID": child.parser.get('procProperties', 'TEMPLATEID')})
                recordList.append(record)
            else:
                data.update({'recordType': 'FAILURE'})
                self.processordoc['failures'].append(data)
        return recordList

    async def validate_mongo_db_rec(self, dataList, **kwargs):
        enrollment = 'SAPHR' if kwargs['enrollment'].upper(
        ) == 'SAPPHIRE' else kwargs['enrollment']
        newDoc = {enrollment: []}
        dataList[0].update({'contractid': None})
        for i in dataList:
            invalid = False
            if not i.get('source') or i['source'].upper() not in ['RBMS', 'SAPPHIRE', 'SAPHR', 'NASCO']:
                i.update(
                    {'errorMessage': 'SOURCE IS MISSING OR NOT RBMS, SAPPHIRE, SAPHR, OR NASCO', 'recordType': 'WARNING'})
                self.processordoc['warnings'].append(i)
                continue
            if i['source'].upper() in ['RBMS', 'SAPPHIRE', 'SAPHR']:
                if not i.get('totalBilledAmt') or not str(i['totalBilledAmt']).replace('.', '').isnumeric() or int(i['totalBilledAmt']) < 1:
                    i.update(
                        {'errorMessage': 'TOTALBILLEDAMT IS EITHER NULL, STRING, OR HAS VALUE LESS THAN OR EQUAL TO ZERO', 'recordType': 'WARNING'})
                    self.processordoc['warnings'].append(i)
                    continue
                if str(i.get('requestType')).upper() in child.parser.get('procProperties', 'REQUEST_TYPE_TO_SKIP', fallback='').upper().strip().split(','):
                    i.update(
                        {'errorMessage': 'REQUEST TYPE IS SET TO BE SKIPPED FROM CONFIG FILE', 'recordType': 'WARNING'})
                    self.processordoc['warnings'].append(i)
                    continue
            if i['source'].upper() in ['NASCO']:
                if not i.get('accountBalanceAmt') or not str(i['accountBalanceAmt']).replace('.', '').isnumeric() or int(i['accountBalanceAmt']) < 1:
                    i.update(
                        {'errorMessage': 'ACCOUNTBALANCEAMT IS EITHER NULL,STRING, OR HAS VALUE LESS THAN OR  EQUAL TO ZERO', 'recordType': 'WARNING'})
                    self.processordoc['warnings'].append(i)
                    continue
            reqField = child.parser.get(
                'procProperties', 'MONGO_REQUIRED_FIELDS', fallback='')
            reqField = [] if not reqField else reqField.strip().split(',')
            for x in reqField:
                if not i.get(x):
                    invalid = True
                    i.update(
                        {'errorMessage': f'REQUIRED FIELD IS MISSING - ({x})'})
            if not invalid:
                newDoc[enrollment].append(i)
        return newDoc

    async def get_active_invoice_read(self, pdoc):
        async with self.semaphore:
            startTime = time.time()
            logIt(self, message='Arguments passed to getactiveinvoices mcow',
                  arguments=json.dumps(pdoc, default=str), logAs='info')
            result = await getActiveInvoices(child.parser).doWork(pdoc, child.dbinstances['mongoDB'])
            logIt(self, message='response from getactiveinvoices mcow: ' +
                  str(result), logAs='debug')
            self.loading.update(1)

            self.auditdoc["MCOWS"].append({
                "name": "getactiveinvoices_read",
                "startTime": startTime,
                "limit & skip": f"{pdoc['limit']} & {pdoc['skip']}",
                "endTime": time.time(),
                "runtime": seconds_to_time_format(time.time() - startTime),
                "error": result.get('error', ''),
                "errorMessage": result.get('errorMessage', '')
            })

            if result['-retCode'] == 9999:
                errorMessage = result.get(
                    'errorMessage', 'ERROR OCCURRED IN GETACTINVOICES MCOW READ')
                self.auditdoc.update({"PPXControlCode": getControlCode(
                    result), 'status': 'ERROR', 'errorMessage': errorMessage})
                logIt(self, message=result.get('error', 'error occured in getnascopayment mcow count'),
                      ppx_controlcode=FxControl['FieldMissing']['ControlCode'], logAs='exception')
                await self.insertauditdoc()
            return result

    async def get_scheduled_payments(self, pdoc, **kwargs):
        async with self.semaphore:
            startTime = time.time()
            logIt(self, message='Arguments passed to getscheduledpayments mcow',
                  arguments=json.dumps(pdoc, default=str), logAs='info')
            result = await getscheduledpayments(child.parser).dowork(pdoc, child.dbinstances['postgresDB'])
            logIt(self, message='response from getscheduledpayments mcow: ' +
                  str(result), logAs='debug')
            self.loading.update(1)

            self.auditdoc["MCOWS"].append({
                "name": "getscheduledpayments_read",
                "startTime": startTime,
                "limit & offset": f"{kwargs['limit']} & {kwargs['offset']}",
                "endTime": time.time(),
                "runtime": seconds_to_time_format(time.time() - startTime),
                "error": result.get('error', ''),
                "errorMessage": result.get('errorMessage', '')
            })

            if result['-retCode'] == 9999:
                errorMessage = result.get(
                    'errorMessage', 'ERROR OCCURRED IN GETAPOSCHEDULEDPAYMENTS MCOW')
                self.auditdoc.update({"PPXControlCode": getControlCode(
                    result), 'status': 'ERROR', 'errorMessage': errorMessage})
                logIt(self, message=result.get('error', 'error occured in getnascopayment mcow count'),
                      ppx_controlcode=FxControl['FieldMissing']['ControlCode'], logAs='exception')
                await self.insertauditdoc()
            return result

    async def call_email_wrapper_api(self, pdoc):
        async with self.semaphore:
            logIt(self, message='Arguments passed to callEmailWrapperService mcow',
                  arguments=json.dumps(pdoc, default=str), logAs='info')
            result = await callEmailWrapperService(child.parser).doWork(pdoc, child.client)
            logIt(self, message='response from callEmailWrapperService mcow: ' +
                  str(result), logAs='debug')
            self.loading.update(1)
            return result

    async def generateErrorFile(self, bepenv):
        self.processordoc['failures'].extend(self.processordoc['warnings'])
        errorfilename = child.parser.get('procProperties', "ERROR_FILENAME", fallback='NEW_BILL_EMAIL_ERROR_{env}_{epochDateTime}.json').format(
            env=bepenv, epochDateTime=datetime.today().strftime('%Y%m%d%H%M%S'))
        todir = child.parser.get('procProperties', "TO_DIR")
        if os.path.exists(todir):
            pass
        elif not os.path.exists(todir):
            os.makedirs(todir)
        with open(todir + errorfilename, 'w') as fp:
            fp.write(json.dumps(
                self.processordoc['failures'], indent=4, default=str))
        self.auditdoc["fileNames"].append(
            {"fileName": errorfilename, "fileType": "ERROR"})
        logIt(self, message='error file successfully generated', logAs='info')

    async def insertauditdoc(self):
        self.auditdoc["recordsCount"]['warningRecords'] = len(
            self.processordoc['warnings'])
        if self.processordoc['failures'] or self.processordoc['warnings']:
            await self.generateErrorFile(self.bepenv)
        self.auditdoc.update({"endTime": datetime.today().timestamp(), "runtime": seconds_to_time_format(
            datetime.today().timestamp() - self.auditdoc['startTime'])})
        await self.getFileSize()
        pdoc = {"inputjson": self.auditdoc}
        print(json.dumps(pdoc, indent=2))
        for i in self.auditdoc['fileNames']:
            if i.get('fileType') == 'SUCCESS':
                print(f"**FILENAME**{i['fileName']}")

        logIt(self, message='Arguments passed to insertauditdoc mcow - info',
              arguments=pdoc, logAs='info')
        logIt(self, message='calling insertauditlog mcow to insert audit log generated to DB', logAs='info')
        result = await insertauditlog().dowork(pdoc, child.dbinstances['postgresDB'])
        logIt(self, message='response from insertauditdoc mcow:' +
              str(result), logAs='debug')
        if result['-retcode'] == 9999:
            logIt(self, message=result.get(
                'error', 'error occured in insertauditlog mcow'), logAs='exception')
            print('Job exited with 1')
            sys.exit(1)
        elif (self.statusdoc['sysStatus'] == True) or (self.auditdoc['status'] in ['ERROR', 'FAILURE']):
            print('Job exited with 1')
            logIt(self, message="Job exited with 1", logAs='info')
            sys.exit(1)
        else:
            print('Job successfully completed')
            logIt(self, message="Job successfully completed", logAs='info')
            sys.exit(0)

    async def getFileSize(self):
        for i in range(len(self.auditdoc["fileNames"])):
            extension = str(
                self.auditdoc["fileNames"][i]['fileName']).split('.')
            fromDir = child.parser.get('procProperties', 'FROM_DIR') if self.auditdoc["fileNames"][i]['fileType'].upper() in [
                'INPUT'] else child.parser.get('procProperties', 'TO_DIR')
            fileSizeType = child.parser.get('procProperties', 'INBOUND_FILE_SIZE_TYPE') if child.parser.has_option(
                'procProperties', 'INBOUND_FILE_SIZE_TYPE') else 'Bytes'
            decimalPlaces = int(child.parser.get('procProperties', 'DECIMAL_PLACES')) if child.parser.has_option('procProperties', 'DECIMAL_PLACES') \
                and child.parser.get('procProperties', 'DECIMAL_PLACES').isnumeric() else 2
            pdoc = {"fromDir": fromDir, "fileName": '.'.join(extension[0:len(extension)-1]),
                    "fileExtension": '.'+extension[len(extension)-1],
                    "fileSizeType": fileSizeType,
                    "decimalPlaces": decimalPlaces}
            result = getinboundfilesize().doWork(pdoc)
            if result['-retCode'] == 0:
                self.auditdoc["fileNames"][i].update(
                    {'fileSize': result['filesize']})


async def init():
    # creating dbpool
    dblist = ["ppxpostgresdb", "ppxmongodb"]
    child.dbinstances = bepdaofactory.getDBInstances(dblist)
    child.dbpool = await child.dbinstances['postgresDB'].createdbpool(child.parser)
    child.dbpool1 = await child.dbinstances['mongoDB'].createdbpool(child.parser)
    conn = aiohttp.TCPConnector(family=socket.AF_INET, ssl=False)
    child.client = await aiohttp.ClientSession(connector=conn).__aenter__()


async def runmethod(**kwargs):
    await child.run(**kwargs)

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(
            description="new-bill-email-proc", fromfile_prefix_chars='@', epilog="Blah Blah Blah.....Will fill out later")
        # Basic Arguments
        basics = parser.add_argument_group(
            "Basic", "Basic Arguments for server selection etc")
        basics.add_argument('--bepenv', '-e', type=str, required=True, choices=['UNIT', 'TEST', 'STAGE', 'PROD'],
                            help='Server to connect. You can also set SERVER environment variable.')
        basics.add_argument('--touchpointid', '-tpid', type=str, required=False,
                            default=None, help='A unique identifier to identify details regarding job')
        basics.add_argument('--rundate', '-rdate', type=int, required=False,
                            default=None, help='Defines which day of payments to be processed')
        basics.add_argument('--jobdays', '-jdays', type=int, required=False,
                            default=None, help='Defines number of days for payments to be processed')
        arguments = vars(parser.parse_args())
        args = parser.parse_known_args(sys.argv[1:])
        args = args[0]
        child = newbillemailprocessor()
        procConfig = '../config/new-bill-email-properties.txt'
        inputparams = {"processName": "NEWBILLEMAIL"}
        # reading common bep properties config file
        parser = child.propertiesConfiguaration(
            None, inputparams['processName'])
        child.parser = parser
        # creating a logfile
        child.logConfiguaration()
        touchPointId = child.generateTouchPointId(
            args.touchpointid, inputparams['processName'])
        # creating event loops for running processor and connecting to DB and config file creation
        loop = asyncio.get_event_loop()
        loop.run_until_complete(init())
        loop1 = asyncio.get_event_loop()
        loop1.run_until_complete(createPropertiesfile(
            child, inputparams, procConfig, arguments))
        child.parser = child.propertiesConfiguaration(
            procConfig, inputparams['processName'])
        loop2 = asyncio.get_event_loop()
        args = vars(args)
        loop2.run_until_complete(runmethod(**args))
        loop1.close()
        loop2.close()
        loop.close()

    except Exception as ex:
        print(traceback.print_exc())
        print(ex)
        logIt(None, errorMessage=str(ex), logAs='exception')
        print('Job exited with 1')
        sys.exit(1)
