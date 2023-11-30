from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import PersonViewSet, AggregationViewSet

router = DefaultRouter()
router.register('person', PersonViewSet, basename='person')
router.register('aggregation', AggregationViewSet, basename='aggregation')

urlpatterns = [
    path('api/', include(router.urls)),
    # path('api/aggregation/', Aggregation.as_view())
]