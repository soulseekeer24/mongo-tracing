use std::borrow::Borrow;

use mongodb::bson::{Bson, Document};
use mongodb::change_stream::event::ChangeStreamEvent;
use mongodb::change_stream::session::SessionChangeStream;
use mongodb::change_stream::ChangeStream;
use mongodb::error::Result;
use mongodb::options::{
    AggregateOptions, ChangeStreamOptions, CountOptions, CreateIndexOptions, DeleteOptions,
    DistinctOptions, DropCollectionOptions, DropIndexOptions, EstimatedDocumentCountOptions,
    FindOneOptions, FindOptions, InsertManyOptions, InsertOneOptions, ListIndexesOptions,
    ReadConcern, ReplaceOptions, UpdateModifications, UpdateOptions, WriteConcern,
};
use mongodb::results::{
    CreateIndexResult, CreateIndexesResult, DeleteResult, InsertManyResult, InsertOneResult,
    UpdateResult,
};
use mongodb::{ClientSession, Collection, Cursor, Database, IndexModel, SessionCursor};
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
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self)
    )]
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.inner.read_concern()
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self)
    )]
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.inner.write_concern()
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,options)
    )]
    pub async fn drop(&self, options: impl Into<Option<DropCollectionOptions>>) -> Result<()> {
        self.inner.drop(options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,options,session)
    )]
    pub async fn drop_with_session(
        &self,
        options: impl Into<Option<DropCollectionOptions>>,
        session: &mut ClientSession,
    ) -> Result<()> {
        self.inner.drop_with_session(options, session).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,pipeline,options)
    )]
    pub async fn aggregate(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<AggregateOptions>>,
    ) -> Result<Cursor<Document>> {
        self.inner.aggregate(pipeline, options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,pipeline,options,session)
    )]
    pub async fn aggregate_with_session(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<AggregateOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<Document>> {
        self.inner
            .aggregate_with_session(pipeline, options, session)
            .await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,options)
    )]
    pub async fn estimated_document_count(
        &self,
        options: impl Into<Option<EstimatedDocumentCountOptions>>,
    ) -> Result<u64> {
        self.inner.estimated_document_count(options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,filter,options)
    )]
    pub async fn count_documents(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<CountOptions>>,
    ) -> Result<u64> {
        self.inner.count_documents(filter, options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,filter,options,session)
    )]
    pub async fn count_documents_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<CountOptions>>,
        session: &mut ClientSession,
    ) -> Result<u64> {
        self.inner
            .count_documents_with_session(filter, options, session)
            .await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,index,options)
    )]
    pub async fn create_index(
        &self,
        index: IndexModel,
        options: impl Into<Option<CreateIndexOptions>>,
    ) -> Result<CreateIndexResult> {
        self.inner.create_index(index, options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,index,options,session)
    )]
    pub async fn create_index_with_session(
        &self,
        index: IndexModel,
        options: impl Into<Option<CreateIndexOptions>>,
        session: &mut ClientSession,
    ) -> Result<CreateIndexResult> {
        self.inner
            .create_index_with_session(index, options, session)
            .await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,indexes,options)
    )]
    pub async fn create_indexes(
        &self,
        indexes: impl IntoIterator<Item = IndexModel>,
        options: impl Into<Option<CreateIndexOptions>>,
    ) -> Result<CreateIndexesResult> {
        self.inner.create_indexes(indexes, options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,indexes,options,session)
    )]
    pub async fn create_indexes_with_session(
        &self,
        indexes: impl IntoIterator<Item = IndexModel>,
        options: impl Into<Option<CreateIndexOptions>>,
        session: &mut ClientSession,
    ) -> Result<CreateIndexesResult> {
        self.inner
            .create_indexes_with_session(indexes, options, session)
            .await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,query,options)
    )]
    pub async fn delete_many(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
    ) -> Result<DeleteResult> {
        self.inner.delete_many(query, options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,query,options,session)
    )]
    pub async fn delete_many_with_session(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
        session: &mut ClientSession,
    ) -> Result<DeleteResult> {
        self.inner
            .delete_many_with_session(query, options, session)
            .await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,query,options)
    )]
    pub async fn delete_one(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
    ) -> Result<DeleteResult> {
        self.inner.delete_one(query, options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,query,options,session)
    )]
    pub async fn delete_one_with_session(
        &self,
        query: Document,
        options: impl Into<Option<DeleteOptions>>,
        session: &mut ClientSession,
    ) -> Result<DeleteResult> {
        self.inner
            .delete_one_with_session(query, options, session)
            .await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,field_name,filter,options)
    )]
    pub async fn distinct(
        &self,
        field_name: impl AsRef<str>,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<DistinctOptions>>,
    ) -> Result<Vec<Bson>> {
        self.inner.distinct(field_name, filter, options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,field_name,filter,options,session)
    )]
    pub async fn distinct_with_session(
        &self,
        field_name: impl AsRef<str>,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<DistinctOptions>>,
        session: &mut ClientSession,
    ) -> Result<Vec<Bson>> {
        self.inner
            .distinct_with_session(field_name, filter, options, session)
            .await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,name,options)
    )]
    pub async fn drop_index(
        &self,
        name: impl AsRef<str>,
        options: impl Into<Option<DropIndexOptions>>,
    ) -> Result<()> {
        self.inner.drop_index(name, options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,name,options,session)
    )]
    pub async fn drop_index_with_session(
        &self,
        name: impl AsRef<str>,
        options: impl Into<Option<DropIndexOptions>>,
        session: &mut ClientSession,
    ) -> Result<()> {
        self.inner
            .drop_index_with_session(name, options, session)
            .await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,options)
    )]
    pub async fn drop_indexes(&self, options: impl Into<Option<DropIndexOptions>>) -> Result<()> {
        self.inner.drop_indexes(options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,options,session)
    )]
    pub async fn drop_indexes_with_session(
        &self,
        options: impl Into<Option<DropIndexOptions>>,
        session: &mut ClientSession,
    ) -> Result<()> {
        self.inner.drop_indexes_with_session(options, session).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,options)
    )]
    pub async fn list_indexes(
        &self,
        options: impl Into<Option<ListIndexesOptions>>,
    ) -> Result<Cursor<IndexModel>> {
        self.inner.list_indexes(options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,options,session)
    )]
    pub async fn list_indexes_with_session(
        &self,
        options: impl Into<Option<ListIndexesOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<IndexModel>> {
        self.inner.list_indexes_with_session(options, session).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self)
    )]
    pub async fn list_index_names(&self) -> Result<Vec<String>> {
        self.inner.list_index_names().await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,session)
    )]
    pub async fn list_index_names_with_session(
        &self,
        session: &mut ClientSession,
    ) -> Result<Vec<String>> {
        self.inner.list_index_names_with_session(session).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,query,update,options)
    )]
    pub async fn update_many(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
    ) -> Result<UpdateResult> {
        self.inner.update_many(query, update, options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,query,update,options,session)
    )]
    pub async fn update_many_with_session(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
        session: &mut ClientSession,
    ) -> Result<UpdateResult> {
        self.inner
            .update_many_with_session(query, update, options, session)
            .await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,query,update,options,session)
    )]
    pub async fn update_one_with_session(
        &self,
        query: Document,
        update: impl Into<UpdateModifications>,
        options: impl Into<Option<UpdateOptions>>,
        session: &mut ClientSession,
    ) -> Result<UpdateResult> {
        self.inner
            .update_one_with_session(query, update, options, session)
            .await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,pipeline,options)
    )]
    pub async fn watch(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<ChangeStreamOptions>>,
    ) -> Result<ChangeStream<ChangeStreamEvent<T>>>
    where
        T: DeserializeOwned + Unpin + Send + Sync,
    {
        self.inner.watch(pipeline, options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,pipeline,options,session)
    )]
    pub async fn watch_with_session(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<ChangeStreamOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionChangeStream<ChangeStreamEvent<T>>>
    where
        T: DeserializeOwned + Unpin + Send + Sync,
    {
        self.inner
            .watch_with_session(pipeline, options, session)
            .await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,filter,options)
    )]
    pub async fn find(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOptions>>,
    ) -> Result<Cursor<T>> {
        self.inner.find(filter, options).await
    }
    #[instrument(
    fields(
    db.name = % self.info.database_name ,
    db.system = "mongodb",
    db.collection = % self.inner.name(),
    otel.kind = "client",
    ),
    skip(self,filter,options,session)
    )]
    pub async fn find_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<FindOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionCursor<T>> {
        self.inner.find_with_session(filter, options, session).await
    }
}
