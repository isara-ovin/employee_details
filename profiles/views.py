from rest_framework import viewsets
from rest_framework.views import APIView
from django.http import JsonResponse
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.filters import OrderingFilter, SearchFilter
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework.pagination import LimitOffsetPagination
from .models import Person
from .serializers import PersonSerializer #, AggregationSerializer
from .filters import PersonModelFilter
from django.conf import settings

from pyspark.sql.functions import col, current_date, datediff, expr
from pyspark.sql.types import DateType
from pyspark.sql.functions import col, avg, when, lit, format_number


class PersonViewSet(viewsets.ModelViewSet):
    queryset = Person.objects.all()
    serializer_class = PersonSerializer
    filter_backends = [OrderingFilter, SearchFilter, DjangoFilterBackend]
    pagination_class = LimitOffsetPagination
    search_fields = ['first_name', 'last_name', 'email', 'industry']
    filterset_class = PersonModelFilter
    ordering_fields = '__all__'

class AggregationViewSet(viewsets.ViewSet):

    # GET endpoint: /aggregation/
    def list(self, request):
        data = {"message": "Following endpoints are available from the current root",
                "endpoints": ['average_age_per_industry', 'average_age_per_salary']}
        return Response(data)

    # GET endpoint: /aggregation/average_age_per_industry/
    @action(detail=False, methods=['get'])
    def average_age_per_industry(self, request):

        # Fetch required data 
        df = self.create_dataframe(Person, ['industry', 'date_of_birth'])

        # Calculating age
        date_format = "dd/MM/yyyy"
        df = df.withColumn("date_of_birth", expr(f"TO_DATE(date_of_birth, '{date_format}')").cast(DateType()))
        df = df.withColumn("age", expr("year(current_date()) - year(date_of_birth)"))

        response = self.avg_by_industry(df, 'age')
 
        return Response(response)
    
    # GET endpoint: /aggregation/average_age_per_salary/
    @action(detail=False, methods=['get'])
    def average_salary_per_industry(self, request):

        df = self.create_dataframe(Person, ['industry', 'salary'])

        # Removing records where salary is Null
        df = df.na.drop(subset=["salary"])

        response = self.avg_by_industry(df, 'salary')

        return Response(response)

    # GET endpoint: /aggregation/average_salary_per_experiance/
    @action(detail=False, methods=['get'])
    def average_salary_per_experiance(self, request):

        columns = ['salary', 'years_of_experience']
        df = self.create_dataframe(Person, columns)

        response = self.avg_by_industry(df, *columns)

        return Response(response)

    # Following will provide logical support for APIs

    def create_dataframe(self, model, fields):
        # Query from sqlite and create dataframe
        data = model.objects.all().values(*fields)
        df = settings.SPARK.createDataFrame(data)

        return df
   
    def avg_by_industry(self, df, avg_by, group_by='industry'):
        # Calculate counts for response
        null_count = df.filter(col(group_by).isNull()).count()
        row_count = df.count()

        if group_by == 'industry':
            na_count = df.filter(col(group_by) == lit("n/a")).count()
            df = df.withColumn(group_by, when(col(group_by) == lit("n/a"), lit("Not Applicable")).otherwise(col(group_by)))
            df = df.fillna("Other", group_by)  
            message = f'Renamed {na_count} records n/a to Not Applicable and Filled {null_count} Null records as Other'          
        
        if group_by == 'years_of_experience':
            df = df.fillna({'years_of_experience':0, 'salary':0})
            message = f'{null_count} records filled with 0'

        # Calculating the average
        df_avg = df.groupBy(group_by).agg(avg(avg_by).alias('average'))
        df_avg = df_avg.withColumn("average", col('average').cast("double"))
        df_avg = df_avg.withColumn('average', format_number('average', 2))

        # Generating list of dicts
        values = [row.asDict() for row in df_avg.collect()]

        return {'data':values, 'row_count':row_count, 'message':message}