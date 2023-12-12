from rest_framework import generics
from knox.models import AuthToken
from knox.auth import TokenAuthentication
from django.contrib.auth.signals import user_logged_in
from rest_framework.response import Response

from .serializers import UserSerializer


class LoginAPI(generics.GenericAPIView):
    serializer_class = UserSerializer
    authentication = [TokenAuthentication]

    def post(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.validated_data
        AuthToken.objects.filter(user=user).delete()  # удаление старых токенов
        _, token = AuthToken.objects.create(user)
        user_logged_in.send(sender=user.__class__, request=request, user=user)  # отправка о входе
        return Response({
            "user": UserSerializer(user, context=self.get_serializer_context()).data,
            "token": token
        })
