#!/usr/bin/env bash

MONGO_URI="mongodb://mongoadmin:mongoadminsecret@localhost:${BENCHMARK_MONGO_PORT:-27018}"

NED_EXPECTED_COUNT=1872544
EXPECTED_AUX_ALERTS=27948
SNAPSHOT_COLLECTION_NAME="ZTF_alerts_aux_snapshot"
N_FILTERS=25

# Only import NED alerts if the collection does not exist or has the wrong count
NED_COLLECTION_NAME="NED"
NED_COLLECTION_EXISTS=$(mongosh "$MONGO_URI/$DB_NAME?authSource=admin" --quiet --eval "db.getCollectionNames().includes('$NED_COLLECTION_NAME')")
NED_COLLECTION_COUNT=0
if [ "$NED_COLLECTION_EXISTS" = "true" ]; then
    NED_COLLECTION_COUNT=$(mongosh "$MONGO_URI/$DB_NAME?authSource=admin" --quiet --eval "db.getCollection('$NED_COLLECTION_NAME').countDocuments()")
fi
echo "NED collection exists: $NED_COLLECTION_EXISTS"
echo "NED collection count: $NED_COLLECTION_COUNT (expected $NED_EXPECTED_COUNT)"

if [ "$NED_COLLECTION_EXISTS" = "false" ] || [ "${NED_COLLECTION_COUNT:-0}" -ne "$NED_EXPECTED_COUNT" ]; then
    if [ "$NED_COLLECTION_EXISTS" = "true" ]; then
        echo "NED collection exists but has wrong count ($NED_COLLECTION_COUNT != $NED_EXPECTED_COUNT); dropping and reimporting"
        mongosh "$MONGO_URI/$DB_NAME?authSource=admin" \
            --quiet --eval "db.$NED_COLLECTION_NAME.drop()"
    fi

    echo "Creating collection $NED_COLLECTION_NAME"
    mongosh "$MONGO_URI/$DB_NAME?authSource=admin" \
        --eval "db.createCollection('$NED_COLLECTION_NAME')"

    echo "Creating 2d index on coordinates.radec_geojson for $NED_COLLECTION_NAME"
    mongosh "$MONGO_URI/$DB_NAME?authSource=admin" \
        --eval "db.$NED_COLLECTION_NAME.createIndex({ 'coordinates.radec_geojson': '2dsphere' })"

    echo "Importing NED alerts into $DB_NAME MongoDB database"
    gunzip -kc /kowalski.NED.json.gz | \
        mongoimport \
        "$MONGO_URI/$DB_NAME?authSource=admin$DB_ADD_URI" \
        --collection $NED_COLLECTION_NAME \
        --jsonArray

    NED_COLLECTION_COUNT=$(mongosh "$MONGO_URI/$DB_NAME?authSource=admin" --quiet --eval "db.getCollection('$NED_COLLECTION_NAME').countDocuments()")
    if [ "${NED_COLLECTION_COUNT:-0}" -ne "$NED_EXPECTED_COUNT" ]; then
        echo "Failed to import NED alerts: expected $NED_EXPECTED_COUNT documents but got $NED_COLLECTION_COUNT"
        exit 1
    fi
else
    echo "NED alerts already imported with correct count ($NED_COLLECTION_COUNT); skipping import"
fi

# Drop all mutable collections in a single mongosh call. ZTF_alerts_aux_snapshot
# is deliberately preserved across runs so that subsequent setups (in the same
# Mongo data dir) can restore ZTF_alerts_aux from it without re-running
# mongorestore on the 2.8GB gzipped archive.
mongosh "$MONGO_URI/$DB_NAME?authSource=admin" --quiet --eval "
    db.ZTF_alerts.drop();
    db.ZTF_alerts_aux.drop();
    db.ZTF_alerts_cutouts.drop();
    db.filters.drop();"

# Create the filters collection + filter_id index, then bulk-insert N_FILTERS
# copies of cats150 in a single mongosh invocation. The previous implementation
# spawned one mongosh process per filter (and two jq processes each), which
# cost ~30-50s of JVM/mongosh startup on top of the actual inserts.
echo "Ingesting $N_FILTERS copies of the cats150 filter into filters collection (batched)"
mongosh "$MONGO_URI/$DB_NAME?authSource=admin" --quiet --eval "
    const fs = require('fs');
    const baseFilter = JSON.parse(fs.readFileSync('/cats150.filter.json', 'utf8'));
    function readUuid() {
        return fs.readFileSync('/proc/sys/kernel/random/uuid', 'utf8').trim();
    }
    db.createCollection('filters');
    db.filters.createIndex({ filter_id: 1 });
    const docs = [];
    for (let i = 1; i <= $N_FILTERS; i++) {
        const copy = JSON.parse(JSON.stringify(baseFilter));
        copy._id = readUuid();
        copy.name = 'cats150_' + i;
        docs.push(copy);
    }
    const result = db.filters.insertMany(docs);
    print('Inserted ' + Object.keys(result.insertedIds).length + ' filters');
"

# ZTF_alerts_aux restore: prefer the in-server snapshot when present.
# - First-ever setup: snapshot is missing, so mongorestore from the gzipped
#   archive populates ZTF_alerts_aux, then we materialize the snapshot.
# - Subsequent setups (same Mongo data dir): snapshot is already there, so we
#   skip mongorestore entirely and copy ZTF_alerts_aux <- snapshot via $out.
#   The server-side $out copy takes a few seconds vs ~30-60s for mongorestore.
SNAPSHOT_COUNT=$(mongosh "$MONGO_URI/$DB_NAME?authSource=admin" --quiet --eval "
    const target = '$SNAPSHOT_COLLECTION_NAME';
    db.getCollectionNames().includes(target) ? db.getCollection(target).countDocuments() : 0
")
SNAPSHOT_COUNT=${SNAPSHOT_COUNT:-0}

if [ "$SNAPSHOT_COUNT" -eq "$EXPECTED_AUX_ALERTS" ]; then
    echo "$SNAPSHOT_COLLECTION_NAME is present with $SNAPSHOT_COUNT documents; restoring ZTF_alerts_aux from snapshot"
    mongosh "$MONGO_URI/$DB_NAME?authSource=admin" --quiet --eval "
        db.$SNAPSHOT_COLLECTION_NAME.aggregate([{ \$out: 'ZTF_alerts_aux' }]);"
else
    echo "$SNAPSHOT_COLLECTION_NAME missing or stale (count=$SNAPSHOT_COUNT, expected=$EXPECTED_AUX_ALERTS); loading from gzipped archive"
    mongorestore --uri="$MONGO_URI/?authSource=admin" \
        --gzip \
        --archive=/boom_throughput.ZTF_alerts_aux.dump.gz \
        --nsInclude='boom_throughput.ZTF_alerts_aux' \
        --nsFrom='boom_throughput.ZTF_alerts_aux' \
        --nsTo="$DB_NAME.ZTF_alerts_aux"

    echo "Materializing $SNAPSHOT_COLLECTION_NAME for future fast restores"
    mongosh "$MONGO_URI/$DB_NAME?authSource=admin" --quiet --eval "
        db.ZTF_alerts_aux.aggregate([{ \$out: '$SNAPSHOT_COLLECTION_NAME' }]);"
    SNAPSHOT_COUNT=$(mongosh "$MONGO_URI/$DB_NAME?authSource=admin" --quiet --eval "db.getCollection('$SNAPSHOT_COLLECTION_NAME').countDocuments()")
    if [ "${SNAPSHOT_COUNT:-0}" -ne "$EXPECTED_AUX_ALERTS" ]; then
        echo "Failed to build $SNAPSHOT_COLLECTION_NAME: expected $EXPECTED_AUX_ALERTS but got $SNAPSHOT_COUNT"
        exit 1
    fi
    echo "Built $SNAPSHOT_COLLECTION_NAME with $SNAPSHOT_COUNT documents"
fi

mongosh "$MONGO_URI/$DB_NAME?authSource=admin" \
    --eval "db.ZTF_alerts_aux.createIndex({ 'coordinates.radec_geojson': '2dsphere' })"

# Verify the freshly-restored ZTF_alerts_aux has the expected document count.
ACTUAL_AUX_ALERTS=$(mongosh "$MONGO_URI/$DB_NAME?authSource=admin" --quiet --eval "db.getSiblingDB('$DB_NAME').ZTF_alerts_aux.countDocuments()")
if [ "$ACTUAL_AUX_ALERTS" -ne "$EXPECTED_AUX_ALERTS" ]; then
    echo "Expected $EXPECTED_AUX_ALERTS documents in ZTF_alerts_aux collection, but found $ACTUAL_AUX_ALERTS"
    exit 1
else
    echo "ZTF_alerts_aux ready with $ACTUAL_AUX_ALERTS documents"
fi

echo "MongoDB initialization script completed successfully"
exit 0
