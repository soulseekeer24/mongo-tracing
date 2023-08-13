use std::borrow::Borrow;

use mongodb::bson::Document;
use mongodb::error::Result;
use mongodb::options::{
    FindOneOptions, InsertManyOptions, InsertOneOptions, ReplaceOptions, UpdateModifications,
    UpdateOptions,
};
use mongodb::results::{InsertManyResult, InsertOneResult, UpdateResult};
use mongodb::{ClientSession, Collection, Database};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::instrument;

struct CollectionInfo {
    database_name: String,
}

pub trait InstrumentedCollectionExt {
    fn collection_instrumented<T>(&self, name: &str) -> InstrumentedCollection<T>;
}

impl InstrumentedCollectionExt for Database {
    fn collection_instrumented<T>(&self, name: &str) -> InstrumentedCollection<T> {
        InstrumentedCollection {
            info: CollectionInfo {
                database_name: self.name().parse().unwrap(),
            },
            inner: self.collection(name),
        }
    }
}

pub struct InstrumentedCollection<T> {
    info: CollectionInfo,
    inner: Collection<T>,
}

impl<T> InstrumentedCollection<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self, filter, options)
    )]
    pub async fn find_one(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOneOptions>>,
    ) -> Result<Option<T>> {
        self.inner.find_one(filter, options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self, filter, options,session)
    )]
    pub async fn find_one_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOneOptions>>,
        session: &mut ClientSession,
    ) -> Result<Option<T>> {
        self.inner
            .find_one_with_session(filter, options, session)
            .await
    }
}

impl<T> InstrumentedCollection<T>
where
    T: Serialize,
{
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self, docs, options)
    )]
    pub async fn insert_many(
        &self,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        options: impl Into<Option<InsertManyOptions>>,
    ) -> Result<InsertManyResult> {
        self.inner.insert_many(docs, options).await
    }

    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self, docs, options,session)
    )]
    pub async fn insert_many_with_session(
        &self,
        docs: impl IntoIterator<Item = impl Borrow<T>>,
        options: impl Into<Option<InsertManyOptions>>,
        session: &mut ClientSession,
    ) -> Result<InsertManyResult> {
        self.inner
            .insert_many_with_session(docs, options, session)
            .await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self, doc, options)
    )]
    pub async fn insert_one(
        &self,
        doc: impl Borrow<T>,
        options: impl Into<Option<InsertOneOptions>>,
    ) -> Result<InsertOneResult> {
        self.inner.insert_one(doc, options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self, doc, options,session)
    )]
    pub async fn insert_one_with_session(
        &self,
        doc: impl Borrow<T>,
        options: impl Into<Option<InsertOneOptions>>,
        session: &mut ClientSession,
    ) -> Result<InsertOneResult> {
        self.inner
            .insert_one_with_session(doc, options, session)
            .await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self, query, replacement,options)
    )]
    pub async fn replace_one(
        &self,
        query: Document,
        replacement: impl Borrow<T>,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> Result<UpdateResult> {
        self.inner.replace_one(query, replacement, options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self, query, replacement,options,session)
    )]
    pub async fn replace_one_with_session(
        &self,
        query: Document,
        replacement: impl Borrow<T>,
        options: impl Into<Option<ReplaceOptions>>,
        session: &mut ClientSession,
    ) -> Result<UpdateResult> {
        self.inner
            .replace_one_with_session(query, replacement, options, session)
            .await
    }
}

impl<T> InstrumentedCollection<T> {
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self, query, update,options)
    )]
    pub async fn update_one(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
    ) -> Result<UpdateResult> {
        self.inner.update_one(query, update, options).await
    }
}
