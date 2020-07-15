from rest_framework import serializers

# from .exception import ExportPlanRegionNotFound
# from unicharm.models import Region

DATE_FILTER_PATTERN = ('%d-%m-%Y',)


class ExportByDateInputSerializer(serializers.Serializer):
    start_date = serializers.DateField(input_formats=DATE_FILTER_PATTERN,required=False)
    end_date = serializers.DateField(input_formats=DATE_FILTER_PATTERN,required=False)
    province_code =   serializers.CharField(required=False)
    campaign_sid = serializers.CharField(required=False)
    store_sid = serializers.CharField(required=False)
    pg_sid = serializers.CharField(required=False)
    export_type = serializers.CharField(required=True)


class ExportBydateSerializer(serializers.Serializer):
    export_file_url = serializers.CharField()
