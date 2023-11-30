from rest_framework import serializers
from .models import Person

class PersonSerializer(serializers.ModelSerializer):
    class Meta:
        model = Person
        fields = '__all__'


# class AggregationSerializer(serializers.Serializer):
#     total_count = serializers.IntegerField()
#     ignored_count = serializers.IntegerField()
#     considered_count = serializers.IntegerField()
#     average_value = serializers.FloatField()
#     stat_type = serializers.CharField()