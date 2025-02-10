from datetime import timedelta
from rest_framework.viewsets import ViewSet
from rest_framework.response import Response
from rest_framework import status
from SatProductCurator.serializers import NewCollectionRequestSerializer
from IngestionEngine.workers._base_logger import Logger
import logging

log = Logger("NewCollectionRequest").get_logger()


class NewCollectionRequestViewSet(ViewSet):
    def create(self, request):
        log(f"Received a new collection request with data: {request.data}", level=logging.DEBUG,)

        log("Serializing collection request.", level=logging.DEBUG,)
        serializer = NewCollectionRequestSerializer(data=request.data)

        log("Checking if serializer is valid or not.", level=logging.DEBUG,)
        if serializer.is_valid():
            # Add 5 days to end date if start date is not provided
            log(
                "Adding 5 days to end date if start date is not provided.",
                level=logging.DEBUG,
            )
            if not serializer.validated_data.get("EndDate"):
                serializer.validated_data["EndDate"] = serializer.validated_data[
                    "StartDate"
                ] + timedelta(days=5)

            # Check if coordinates are provided and not empty.

            extent_request = request.data.get("Extent")
            log(
                "Checking if coordinates are provided and not empty.",
                level=logging.DEBUG,
                
            )
            if (
                "coordinates" not in extent_request
                or not extent_request["coordinates"]
                or not extent_request["coordinates"][0]
            ):
                response_data = {
                    "code": 400,
                    "message": "Invalid Extent.",
                    "error": "Extent has empty coordinates.",
                    "success": False,
                }
                return Response(response_data, status=status.HTTP_400_BAD_REQUEST)

            # Automatic accept requests for now
            serializer.validated_data["IsAccepted"] = True

            serializer.save()

            log("Serializer saved Successfully.", level=logging.DEBUG,
              )

            new_collection_id = serializer.instance.NewCollectionID

            log("Sending response with new collection id.", level=logging.DEBUG,
               )
            response_data = {
                "code": 201,
                "message": "New Collection Request for MnA initiated successfully.",
                "new_collection_id": new_collection_id,
                "success": "true",
            }
            log("Response sent successfully.", level=logging.DEBUG)
            return Response(response_data, status=status.HTTP_201_CREATED)
        else:
            response = {
                "code": 400,
                "message": "Invalid request data.",
                "error": serializer.errors,
                "success": "false",
            }
            return Response(response, status=status.HTTP_400_BAD_REQUEST)
