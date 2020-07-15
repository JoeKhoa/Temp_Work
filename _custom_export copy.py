import json

from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import generics
from rest_framework.parsers import JSONParser, MultiPartParser
from rest_framework.renderers import BrowsableAPIRenderer, JSONRenderer
from rest_framework.response import Response

from apis.v1.authentications import JWTAuthentication
from apis.v1.permissions import AdminAccessPermission
from commons.paths import ensure_dir, get_local_file_full_path
from unicharm.export_custom_data._export_custom_data import ExportPlan, ExportCustomerReport
from unicharm.loggers import admin_logger
from unicharm.models import ExportData, Plan

from .exception import ExportPlanInvalidError
from .fields import EXPORT_TAG
from .serializers import ExportByDateInputSerializer, ExportBydateSerializer

logger = admin_logger

__all__ = ['ExportByDateAPIView']


class ExportByDateAPIView(generics.ListAPIView):
    authentication_classes = [JWTAuthentication]
    permission_classes = (AdminAccessPermission,)

    parser_classes = (MultiPartParser, JSONParser)
    renderer_classes = [JSONRenderer, BrowsableAPIRenderer]
    serializer_class = ExportBydateSerializer
    input_serializer_class = ExportByDateInputSerializer
    

    queryset = ExportData.objects

    @swagger_auto_schema(
        manual_parameters=[
            openapi.Parameter(
                name='Authorization',
                in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                required=True,
                description='Bearer <token>'
            ),
            openapi.Parameter(
                name='start_date',
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                required=False,
                description='Start date, ex: 01-07-2020'
            ),
            openapi.Parameter(
                name='end_date',
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                required=False,
                description='To date, ex: 30-07-2020'
            ),
            openapi.Parameter(
                name='province_code',
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                required=False,
                description='province_code'
            ),
            openapi.Parameter(
                name='campaign_sid',
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                required=False,
                description='campaign_sid'
            ),
                openapi.Parameter(
                name='store_sid',
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                required=False,
                description='store_sid'
            ),
                openapi.Parameter(
                name='pg_sid',
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                required=False,
                description='pg_sid'
            ),
                openapi.Parameter(
                name='export_type',
                in_=openapi.IN_QUERY,
                type=openapi.TYPE_STRING,
                enum=['BY_DATE', 'BY_CUSTOMER'],
                required=True,
            ),
        ],
        responses={
            200: ExportBydateSerializer,
        },
        tags=[EXPORT_TAG],
        operation_id='Export custom'
    )
    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        data = request.query_params
        # return Response(str(data))
        serializer = self.input_serializer_class(data=data)
        if not serializer.is_valid():
            raise ExportPlanInvalidError(detail=serializer.errors)
        data = serializer.data
        new_export_file_path = ExportData.new_export_file_path(
            ExportPlan.export_plan_filename + ExportPlan.export_plan_file_ext)
        export_data = ExportData(inputs=json.dumps(data))
        export_data.save()


        if data.get('export_type') == 'BY_DATE':
            data = ExportPlan.export_plan(
            data.get('start_date'),
            data.get('end_date'),
            data.get('province_code'),
            str(data.get('campaign_sid')).replace("-","") if data.get('campaign_sid') else '',
            str(data.get('store_sid')).replace("-","") if data.get('store_sid') else '',
            str(data.get('pg_sid')).replace("-","") if data.get('pg_sid') else ''        
            )
            new_export_file_full_path = get_local_file_full_path(
                new_export_file_path)
            ensure_dir(new_export_file_full_path)
            writed, errors = ExportPlan.write_data_to_file(
                data, new_export_file_full_path)
            if writed is True:
                export_data.status = ExportData.DONE
                export_data.export_file.name = new_export_file_path
            else:
                export_data.status = ExportData.FAILED
                export_data.reason = errors

            export_data.save()
            serializer = ExportBydateSerializer(export_data)
            return Response(serializer.data)    
            
        elif data.get('export_type') == 'BY_CUSTOMER':

            data = ExportPlan.export_plan(
            data.get('start_date'),
            data.get('end_date'),
            data.get('province_code'),
            str(data.get('campaign_sid')).replace("-","") if data.get('campaign_sid') else '',
            str(data.get('store_sid')).replace("-","") if data.get('store_sid') else '',
            str(data.get('pg_sid')).replace("-","") if data.get('pg_sid') else ''        
            )
            new_export_file_full_path = get_local_file_full_path(
                new_export_file_path)
            ensure_dir(new_export_file_full_path)
            writed, errors = ExportCustomerReport.write_data_to_file(
                data, new_export_file_full_path)
            if writed is True:
                export_data.status = ExportData.DONE
                export_data.export_file.name = new_export_file_path
            else:
                export_data.status = ExportData.FAILED
                export_data.reason = errors
            export_data.save()
            serializer = ExportBydateSerializer(export_data)
            return Response(serializer.data)         