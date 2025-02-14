#!/usr/bin/env python3

from datetime import datetime
from typing import List

# from clickhouse_sqlalchemy import make_session, types
from sqlalchemy import ForeignKey, func, Index, Boolean
from sqlalchemy import Integer, String, DateTime, VARCHAR
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.expression import select, and_

from common import (Base, CDX_URL_TEMPLATE, UrlPattern, make_engine,
                    sa_session, tz_utz)
from config import (CC_DATA_HOSTNAME, TABLE_ARGS_Crawl,
                    TABLE_DEFAULTS_CdxFirstUrl,
                    TABLE_DEFAULTS_WebTextEmbeddings,
                    TABLE_DEFAULTS_WarcRecord)
from config import (COL_SMALLINT, COL_INTEGER, COL_BIGINT, COL_FLOAT,
                    COL_TIMESTAMP, COL_STRING, COL_TEXT, COL_BOOLEAN,
                    COL_JSON, COL_ARRAY, COL_ENUM)

import pudb

class Crawl(Base):
    __tablename__ = "crawl"
    __table_args__ = TABLE_ARGS_Crawl

    """ Crawl identity: the label is the 'CC-MAIN-YYY-WW' part of the URL"""
    id: Mapped[int] = mapped_column(
        COL_BIGINT, primary_key=True,
        server_default=func.floor(2**32 * func.random()))
    label: Mapped[str] = mapped_column(COL_STRING(20))
    url: Mapped[str] = mapped_column(VARCHAR(512))
    created: Mapped[datetime] = mapped_column(DateTime(timezone=tz_utz),
                                              server_default=func.now())

    @classmethod
    def get(cls, label=None, crawl_id=None):
        # Break out shortened column names
        c = cls.__table__.c
        with sa_session() as sa:
            if label:
                res = (sa.execute(select('*')
                                   .select_from(cls)
                                   .where(c.label == label)
                                   .limit(1))
                       .mappings().first())
            elif crawl_id:
                res = (sa.execute(select('*')
                                   .select_from(cls)
                                   .where(c.crawl_id == crawl_id)
                                   .limit(1))
                       .mappings().first())
            else:
                raise ValueError("Must provide either label or crawl_id")
        if res:
            return cls(**res)
        return None

    @classmethod
    def exists(cls, label):
        return cls.get(label=label)

    @classmethod
    def add(cls, label, url):
        with sa_session() as sa:
            sa.execute(cls.__table__.insert(),
                                 {"label": label, "url": url})
            sa.commit()
            return (sa.query(cls)
                              .filter_by(label=label, url=url)
                              .first())

    def __repr__(self) -> str:
        return (f"Crawl(id={self.id}, "
                f"label={self.label}, "
                f"url={self.url!r}, "
                f"created={self.created!r}")

    __str__ = __repr__


class CdxFirstUrl(Base):
    __tablename__ = "cdx_start_url"
    __table_args__ = TABLE_DEFAULTS_CdxFirstUrl

    """ CdxFirstUrl: contains the first URL in a CC cdx index file, and this
        table serves as a binary-searchable index to locate the cdx file
        in which a domain's URLs are stored."""
    id: Mapped[int] = mapped_column(COL_BIGINT, primary_key=True,
                                    server_default=func.floor(2**63 * func.random()))
    crawl_id: Mapped[int] = mapped_column(ForeignKey("crawl.id"),
                                          nullable=False)
    cdx_num: Mapped[int] = mapped_column(Integer, nullable=False)
    tld: Mapped[str] = mapped_column(VARCHAR(64), nullable=False)
    domain: Mapped[str] = mapped_column(VARCHAR(128), nullable=False)
    subdomain: Mapped[str] = mapped_column(VARCHAR(128),
                                           nullable=False)
    path: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(COL_TIMESTAMP,
                                                nullable=False)
    headers: Mapped[str] = mapped_column(VARCHAR(511), nullable=False)
    created: Mapped[datetime] = mapped_column(DateTime(timezone=tz_utz),
                                              server_default=func.now())

    def __repr__(self) -> str:
        return (f"CdxFirstUrl=({self.id!r}, "
                f"crawl_id={self.crawl_id!r}, "
                f"cdx_num={self.cdx_num!r}, "
                f"tld={self.tld!r}, "
                f"domain={self.domain!r}, "
                f"subdomain={self.subdomain!r}, "
                f"path={self.path!r}, "
                f"headers={self.headers!r}, "
                f"created={self.created!r})")

    __str__ = __repr__

    def to_cdx_url(self):
        crawl = Crawl.get(crawl_id=self.crawl_id)
        return CDX_URL_TEMPLATE.format(
            hostname=CC_DATA_HOSTNAME,
            label=crawl.label,
            cdx_num=self.cdx_num
        )

    @classmethod
    def exists(cls, crawl: Crawl):
        with sa_session() as sa:
            return bool(sa.query(cls)
                          .filter_by(crawl_id=crawl.id)
                          .first())

    @classmethod
    def get(cls, crawl: Crawl):
        with sa_session() as sa:
            return list(sa.query(cls).filter_by(crawl_id=crawl.id).all())

    @classmethod
    def count(cls, crawl: Crawl):
        with sa_session() as sa:
            return sa.query(cls).filter_by(crawl_id=crawl.id).count()

    @classmethod
    def add_batch(cls, urls: List["CdxFirstUrl"]):
        with sa_session() as sa:
            sa.bulk_save_objects(urls)
            sa.commit()
            return (sa.query(cls)
                      .filter_by(crawl_id=urls[0].crawl_id)
                      .count())

    @classmethod
    def all(cls):
        with sa_session() as sa:
            for row in sa.query(cls).all():
                yield row

    @classmethod
    def find_domain_cdxes(cls, crawl_id: int, url: UrlPattern):
        # Break out for shortened column names
        c = CdxFirstUrl.__table__.c
        with sa_session() as sa:

            # First cdx number likely to contain the TLD
            tld_cdx0 = sa.execute(
                select(c.crawl_id, c.cdx_num, c.tld, c.domain)
                    .select_from(cls)
                    .where(and_(c.crawl_id == crawl_id,
                                c.tld < url.tld))
                    .order_by(c.tld.desc(), c.domain.desc())
                    .limit(1)
            ).first().cdx_num

            # Last cdx number likely to contain the TLD
            tld_cdx1 = sa.execute(
                select(c.crawl_id, c.cdx_num, c.tld, c.domain)
                    .select_from(cls)
                    .where(and_(c.crawl_id == crawl_id,
                                c.tld == url.tld))
                    .order_by(c.tld.desc(), c.domain.desc())
                    .limit(1)
            ).first().cdx_num

            # First cdx number likely to contain the domain
            dom_cdx0 = sa.execute(
                select(c.crawl_id, c.cdx_num, c.tld, c.domain)
                    .select_from(cls)
                    .where(and_(c.crawl_id == crawl_id,
                                c.domain <= url.domain,
                                c.cdx_num >= tld_cdx0,
                                c.cdx_num <= tld_cdx1))
                    .order_by(c.tld.desc(), c.domain.desc())
                    .limit(1)
            ).first().cdx_num

            # Last cdx number AFTER the cdx likely to contain the domain
            dom_cdx1 = sa.execute(
                select(c.crawl_id, c.cdx_num, c.tld, c.domain)
                    .select_from(cls)
                    .where(and_(c.crawl_id == crawl_id,
                                c.tld >= url.tld,
                                c.domain >= url.domain,
                                c.cdx_num >= tld_cdx0,
                                c.cdx_num <= tld_cdx1))
                    .order_by(c.tld.asc(), c.domain.asc())
                    .limit(1)
            ).first().cdx_num

            # Now we know the range of cdx numbers to return, do so
            results = sa.execute(
                select('*')
                    .select_from(cls)
                    .where(and_(c.crawl_id == crawl_id,
                                c.cdx_num >= dom_cdx0,
                                c.cdx_num <= dom_cdx1))
                ).mappings().all()
            return [cls(**r) for r in results[:-1]]


class WebTextEmbeddings(Base):
    __tablename__ = "web_text_embeddings"
    __table_args__ = TABLE_DEFAULTS_WebTextEmbeddings

    """ WebTextEmbeddings: stores the URL, metadata, and LMM embedding 
        of a web page"""
    id: Mapped[int] = mapped_column(COL_BIGINT, primary_key=True,
                                    server_default=func.floor(2**63 * func.random()))
    url: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    page_metadata: Mapped[str] = mapped_column(String(255))
    embedding: Mapped[List[float]] = mapped_column(COL_ARRAY(COL_FLOAT))
    created: Mapped[datetime] = mapped_column(DateTime(timezone=tz_utz),
                                              server_default=func.now())

    def exists(self, session):
        with sa_session() as sa:
            return bool(sa.query(WebTextEmbeddings)
                                    .filter_by(url=self.url)
                                    .first())

    def __repr__(self) -> str:
        return (f"web_text_embeddings(id={self.id}, "
                f"metadata={self.metadata!r}, "
                f"created={self.created!r})")

    __str__ = __repr__


class KnownUrlPatterns(Base):
    __tablename__ = "known_url_patterns"

    """ KnownUrlPatterns: stores the URL patterns of known web pages"""
    id: Mapped[int] = mapped_column(COL_BIGINT, primary_key=True,
                                    server_default=func.floor(2**63 * func.random()))
    crawl_id: Mapped[int] = mapped_column(ForeignKey("crawl.id"),
                                          nullable=False)
    url: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    pattern: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    warc_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    warc_completed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created: Mapped[datetime] = mapped_column(DateTime(timezone=tz_utz),
                                              server_default=func.now())

    def __repr__(self) -> str:
        return (f"known_url_patterns(id={self.id}, "
                f"url={self.url!r}, "
                f"pattern={self.pattern!r}, "
                f"created={self.created!r})")

    __str__ = __repr__

class WarcResource(Base):
    __tablename__ = "warc_record"
    __table_args__ = TABLE_DEFAULTS_WarcRecord

    """ WarcResource: stores the URL, metadata, and LMM embedding 
        of a web page"""
    id: Mapped[int] = mapped_column(COL_BIGINT, primary_key=True,
                                    server_default=func.floor(2**63 * func.random()))
    crawl_id: Mapped[int] = mapped_column(ForeignKey("crawl.id"))
    page_url: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    warc_url: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    page_metadata: Mapped[str] = mapped_column(VARCHAR(511))
    page_length: Mapped[int] = mapped_column(Integer, nullable=False)
    warc_length: Mapped[int] = mapped_column(Integer, nullable=False)
    # embedding: Mapped[List[float]] = mapped_column(types.Array(types.Float32))
    created: Mapped[datetime] = mapped_column(DateTime(timezone=tz_utz),
                                              server_default=func.now())

    @classmethod
    def add(cls, crawl: Crawl, page_url, warc_url, metadata, length) -> None:
        with sa_session() as sa:
            sa.execute(cls.__table__.insert(),
                       {"crawl_id": crawl.id,
                        "page_url": page_url,
                        "warc_url": warc_url,
                        "metadata": metadata,
                        "page_length": int(metadata["length"]),
                        "warc_length": length})
            print("Added ", page_url, "***", warc_url, "*", metadata, "*", length)
            sa.commit()

    @classmethod
    def exists(cls, page_url):
        with sa_session() as sa:
            return bool(sa.query(cls)
                          .filter_by(page_url=page_url)
                          .first())

    def __repr__(self) -> str:
        return (f"warc_record(id={self.id}, "
                f"page_url={self.page_url!r}, "
                f"warc_url={self.warc_url!r}, "
                f"metadata={self.metadata!r}, "
                f"created={self.created!r})")

    __str__ = __repr__


if __name__ == "__main__":
    engine = make_engine(echo=True)
    with sa_session(engine) as sa:
        Base.metadata.create_all(engine)
        sa.commit()
        del engine
