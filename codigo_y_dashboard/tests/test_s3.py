import logging

import boto3
from moto import mock_aws

import config
import s3_publish


def test_sin_bucket_se_omite_con_log(caplog):
    with caplog.at_level(logging.INFO):
        r = s3_publish.publicar(env={})
    assert r["estado"] == "omitido" and "S3 OMITIDO" in caplog.text


def test_publica_en_s3_emulado_con_moto(datos, monkeypatch):
    # moto emula S3 en memoria: valida el uso de boto3, no una cuenta AWS real
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        boto3.client("s3").create_bucket(Bucket="sig-test-bucket")
        r = s3_publish.publicar(env={"S3_BUCKET": "sig-test-bucket"})
        keys = [o["Key"] for o in boto3.client("s3").list_objects_v2(Bucket="sig-test-bucket")["Contents"]]
    assert r["estado"] == "publicado" and r["archivos"] == len(keys) == len(list(config.raw_dir().glob("*")))
    assert all("/bronze/" in k for k in keys)
