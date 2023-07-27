import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import requests
import os
import datetime

def lambda_handler(event, context):
  processingDate = datetime.date.today() + datetime.timedelta(days=2)
  partitionDate = processingDate.strftime("%Y%m%d") 
  print('Executing VirgoAutomate for ' + partitionDate)
  dynamodb = boto3.resource('dynamodb')
  queryResponse = dynamodb.Table('VirgoAutomate_bookings').scan(
    FilterExpression='partitionDate = :partitionDate AND bookingStatusResult = :bookingStatusResult',
    ExpressionAttributeValues={
      ':partitionDate': partitionDate,
      ':bookingStatusResult': ''
    }
  )

  bookings = queryResponse['Items']
    
  for booking in bookings:
    print("--- START BOOKING " + booking['bookingId'])
    url = "https://api-exerp.mywellness.com/core/calendarevent/" + booking['eventId'] +"/book?_c=it-IT"
    payload = json.dumps({
      "userId": booking['userId'],
      "partitionDate": booking['partitionDate']
    })
    headers = {
      'Accept': 'application/json',
      'Accept-Language': 'it-IT,it;q=0.9',
      'User-Agent': 'MywellnessSuperCustom/3 CFNetwork/1399 Darwin/22.1.0',
      'X-MWAPPS-CLIENT': 'mywellnessappios40',
      'X-MWAPPS-APPID': 'ec1d38d7-d359-48d0-a60c-d8c0b8fb9df9',
      'Connection': 'keep-alive',
      'x-mwapps-tz-olson': 'Europe/Rome',
      'Accept-Encoding': 'gzip, deflate, br',
      'X-MWAPPS-OAUTHTOKEN': '20230225104200|74d91c0fc9f304e8b9aa263477bc17a58cdf0b38',
      'X-MWAPPS-CLIENTVERSION': '6.0.4.3,virginitaly,6.0.4.3,iPhone11.2',
      'Authorization': 'Bearer ' + booking['token'],
      'Content-Type': 'application/json',
      'Cookie': '_mwappseu=ec1d38d7-d359-48d0-a60c-d8c0b8fb9df9|MjAyMzAyMjUxODIzMzJ8YTNiN2Q1MDBjZDk1NGVkN2FhZjJmNDI5M2U4MzUxOTl8ZWMxZDM4ZDdkMzU5NDhkMGE2MGNkOGMwYjhmYjlkZjl8MXxXLiBFdXJvcGUgU3RhbmRhcmQgVGltZXxpdC1JVHw1ZjBjY2NmM2FlNWQ0YzRlOTY0NDMwM2ZkOTQzMDE5Nnx8fHwxfDF8MHwxMDB8fHw1OHw4Mjg5fDB8aXQudmlyZ2luYWN0aXZl0.F1369F3DF0E8620F65B4D3CF9A5DA19DC33CC85E5517D9DAEDDA14C9FCBDAFCFCB8CCD3F024757AFBC134A2D403D04721BD87223E78A2E407470EA471DAD74A7'
    }
    print("Request headers: " + str(headers))
    print("Request payload:" + str(payload))
    
    response = requests.request("POST", url, headers=headers, data=payload)
    response.raise_for_status()
    print("Response status code: " + str(response.status_code))
    print("Response payload: " + response.text)
    jsonResponse = response.json()
    updateBookingResponse = dynamodb.Table('VirgoAutomate_bookings').update_item(
      Key={
        'bookingId': booking['bookingId'],
        'partitionDate': booking['partitionDate']
      },
      UpdateExpression='SET bookingStatusResult = :r',
      ExpressionAttributeValues={':r': jsonResponse['data']},
      ReturnValues="UPDATED_NEW"
    )
    try:
      sendEmailResponse = boto3.client('ses').send_email(
          Destination={
              'ToAddresses': ['m@rght.it']
          },
          Message={
              'Body': {
                  'Text': {
                      'Charset': 'UTF-8',
                      'Data': 'Prenotato'
                  }
              },
              'Subject': {
                  'Charset': 'UTF-8',
                  'Data': 'Prenotazione'
              }
          },
          Source='virgoautomate@rght.it'
      )
      print(f"Email sent to {'m@rght.it'}: {sendEmailResponse['MessageId']}")
    except ClientError as e:
      print(f"Error sending email: {e.sendEmailResponse['Error']['Message']}")
    print('--- END BOOKING' + booking['bookingId'])
    
  return {
      'statusCode': 'OK'
  }
