# Failure scenarios - lab notebook

## 1. Schema Registry rejects a breaking schema change

### Setup

Register schema as v1, then apply the FULL compatibility rule on it.

#### Register v1 of order-placed under the subject "orders-placed-value"

```shell
curl -s -X POST \
    -H "Content-Type: application/json; artifactType=AVRO" \
    --data @schemas/order-placed.avsc \
    http://localhost:8080/apis/registry/v2/groups/default/artifacts/orders-placed-value/versions | jq .
#Output:    
#{
#  "name": "OrderPlaced",
#  "createdBy": "",
#  "createdOn": "2026-05-19T13:44:21+0000",
#  "modifiedBy": "",
#  "modifiedOn": "2026-05-19T13:44:21+0000",
#  "id": "orders-placed-value",
#  "version": "1",
#  "type": "AVRO",
#  "globalId": 1,
#  "state": "ENABLED",
#  "contentId": 1,
#  "references": []
#}
```

#### Set FULL compatibility

```shell
# set compatibility mode
curl -s -X POST \
    -H "Content-Type: application/json" \
    --data '{"type":"COMPATIBILITY","config":"FULL"}' \
    http://localhost:8080/apis/registry/v2/groups/default/artifacts/orders-placed-value/rules
    
# verify compatibility rule is set
curl -s -X GET \
    -H "Content-Type: application/json" \
    http://localhost:8080/apis/registry/v2/groups/default/artifacts/orders-placed-value/rules/COMPATIBILITY
#Output:
#{"config":"FULL","type":"COMPATIBILITY"}%      
```

### Failure

We rename the orderId field to id and try to register the result as v2 of the same subject.

![breaking-change-diff.png](breaking-change-diff.png)

#### Try to register a breaking schema

```shell
curl -s -X POST \
    -H "Content-Type: application/json; artifactType=AVRO" \
    --data @docs/order-placed.v2-breaking.avsc \
    http://localhost:8080/apis/registry/v2/groups/default/artifacts/orders-placed-value/versions | jq .
#Output:
#{
#  "causes": [
#    {
#      "description": "id",
#      "context": "/fields/0"
#    },
#    {
#      "description": "orderId",
#      "context": "/fields/0"
#    }
#  ],
#  "message": "Incompatible artifact: orders-placed-value [AVRO], num of incompatible diffs: {2}, list of diff types: [id at /fields/0, orderId at /fields/0] Causes: id at /fields/0, orderId at /fields/0",
#  "error_code": 409,
#  "detail": "RuleViolationException: Incompatible artifact: orders-placed-value [AVRO], num of incompatible diffs: {2}, list of diff types: [id at /fields/0, orderId at /fields/0] Causes: id at /fields/0, orderId at /fields/0",
#  "name": "RuleViolationException"
#}
```

Apicurio returned **two** diffs because FULL compatibility checks both directions:

- **Backward** (new readers, old data): the new schema requires `id` with no default,
  so a new reader cannot deserialize a record that only has `orderId`. → `id at /fields/0`
- **Forward** (old readers, new data): the old schema requires `orderId` with no default,
  so an old reader cannot deserialize a record that only has `id`. → `orderId at /fields/0`

A field rename is the textbook example of a change that fails both directions, which is
why a registry with FULL compatibility refuses it.

### Production lessons from this scenario

- **`AUTO_REGISTER_ARTIFACT=true`** in the producer (which our Java producer has) would have moved
  this same 409 from CI/deploy time to message-send time in production. In a real system this rule
  should be off; schemas get registered through the CI/CD pipeline, not from running applications.
- **Per-artifact vs registry-global rules.** We set FULL on `orders-placed-value` only. The other
  subject `customer-profiles-value` has no rule, so an equally broken evolution there would be
  accepted silently. A production registry should have a default compatibility rule at the registry
  level, with per-subject overrides only where stricter or looser is justified.

<!-- TODO: ## 2. Cross-language Avro through a shared registry — embed running-java-consumer-and-python-validator-for-records.png + 2 sentences on the round-trip -->
