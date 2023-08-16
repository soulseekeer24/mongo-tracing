# MONGO TRACING

This library wrap the Rust MongoDB Driver's Collection object with Telemetry

## Usage

```rust
// Instead of getting collection from database 
    database
        .collection::<Document>("medicalRecordsHistories")
        
// import the Trait 
    use mongo_tracing::InstrumentedCollectionExt;        
 ....
 
 //And replace the "collection" method for "collection_instrumented
 
   database
        .collection_instrumented::<Document>("medicalRecordsHistories")
   

```