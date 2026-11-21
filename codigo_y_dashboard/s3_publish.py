"""Publicacion opcional en Amazon S3 (boto3). Solo actua con S3_BUCKET + credenciales AWS; si no, lo informa y omite.

S3_BUCKET, S3_PREFIX (defecto cds-energia-sig), credenciales por cadena estandar de boto3.
"""
import logging
import os
from datetime import date
from pathlib import Path

import config

log = logging.getLogger("s3")


def publicar(env=None):
    env = os.environ if env is None else env
    bucket = env.get("S3_BUCKET")
    if not bucket:
        log.info("S3 OMITIDO: variable S3_BUCKET no definida")
        return {"estado": "omitido", "motivo": "sin S3_BUCKET"}
    import boto3
    sesion = boto3.Session()
    if sesion.get_credentials() is None:
        log.warning("S3 OMITIDO: S3_BUCKET=%s pero no hay credenciales AWS", bucket)
        return {"estado": "omitido", "motivo": "sin credenciales AWS"}
    s3 = sesion.client("s3")
    prefijo = f"{env.get('S3_PREFIX') or 'cds-energia-sig'}/{date.today().isoformat()}"
    archivos = [(p, f"{prefijo}/{capa}/{p.name}") for capa, d in
                (("bronze", config.raw_dir()), ("silver", config.silver_dir()), ("gold", config.gold_dir()))
                for p in sorted(Path(d).glob("*")) if p.is_file()]
    html = config.out_dir() / "dashboard_interactivo.html"
    if html.exists():
        archivos.append((html, f"{prefijo}/dashboard/{html.name}"))
    for p, key in archivos:
        s3.upload_file(str(p), bucket, key)
    log.info("S3: %d archivos subidos a s3://%s/%s", len(archivos), bucket, prefijo)
    return {"estado": "publicado", "bucket": bucket, "prefijo": prefijo, "archivos": len(archivos)}
