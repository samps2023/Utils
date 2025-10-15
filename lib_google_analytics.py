import os
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import itertools
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gacredentials.json'

class GoogleAnalytics:
    client = BetaAnalyticsDataClient()
    page_dimension_list=['date','country','pageLocation','sessionSource','sessionMedium','sessionCampaignName','sessionDefaultChannelGroup','sessionManualTerm']
    page_metrics_list =['newUsers','activeUsers','scrolledUsers','totalUsers','sessions','engagedSessions','bounceRate','userEngagementDuration','screenPageViews','averageSessionDuration']
    event_dimension_list=['date','country','pageLocation','sessionSource','sessionMedium','sessionCampaignName','sessionDefaultChannelGroup','sessionManualTerm','eventName']
    event_metrics_list=['newUsers','activeUsers','scrolledUsers','totalUsers','sessions','eventCount','eventCountPerUser','eventsPerSession']

    def query_data(self,api_response):
        dimension_headers = [header.name for header in api_response.dimension_headers]
        metric_headers = [header.name for header in api_response.metric_headers]
        dimensions = []
        metrics = []
        for i in range(len(dimension_headers)):
            dimensions.append([row.dimension_values[i].value for row in api_response.rows])
        for i in range(len(metric_headers)):
            metrics.append([row.metric_values[i].value for row in api_response.rows])
        headers = dimension_headers, metric_headers
        headers = list(itertools.chain.from_iterable(headers))   
        data = dimensions, metrics
        data = list(itertools.chain.from_iterable(data))
        df = pd.DataFrame(data)
        df = df.transpose()
        df.columns = headers
        return df
    
    def extract_data(self,category:str=None,dimension:list=None,metric:list= None, **kwargs):
        #select the dimension list and metric list
        dimension =dimension or []
        metric = metric or []
        if category =='page':
            dimension_list = self.page_dimension_list
            metric_list = self.page_metrics_list
        elif category =='event':
            dimension_list = self.event_dimension_list
            metric_list = self.event_metrics_list
        else:
            dimension_list = dimension
            metric_list=metric
        
        if not dimension_list or not metric_list:
            raise ValueError("No dimension and metric given!")
        
        #define the start_date and end_date
        default_date =(datetime.now().date() - relativedelta(days=1)).isoformat()
        start_date = kwargs.get('start_date', default_date)
        end_date = kwargs.get('end_date', default_date) 

        request_api = RunReportRequest(
            property=f"properties/{kwargs.get('propertyId')}",
            dimensions=[Dimension(name=d) for d in dimension_list],
            metrics=[Metric(name=m) for m in metric_list],
                limit=10000,
                offset=0,
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            )
        response = self.client.run_report(request_api,timeout=500)
        df = self.query_data(response)
        
        if 'date' in df:
            df['date'] = pd.to_datetime(df['date'],format='%Y%m%d')
        return df

