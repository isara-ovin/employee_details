import django_filters
from .models import Person

class PersonModelFilter(django_filters.FilterSet):
    
    class Meta:
        model = Person
        fields = {
            'first_name': ['exact', 'icontains'],
            'last_name': ['exact', 'icontains'],
            'email': ['exact', 'icontains'],
            'gender': ['exact'],
            'date_of_birth': ['exact', 'lt'],
            'industry': ['exact'],
            'salary': ['exact', 'gte', 'lte'],
            'years_of_experience': ['exact', 'gt', 'lt'],
        }

