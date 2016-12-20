(ns datomic-schema-adapter.util
  (:require [datomic.api :as d :refer [temp-id]]
            [clojure.set :as set :refer [rename-keys]]))

(def data-entities (keys data-schema))

(defn cross-ref-map [m]
  (hash-map (:ident m) (:ref-ent m)))

(defn ent-idents [coll]
  (mapv #(cross-ref-map %) coll))

(defn ident-by-ent-key
  "Retrieve all idents of data-schema as a vector of strings of a given entity key.
  i.e. passing in :org might return ['name' 'category' 'website' 'ein']"
  [ent-key]
  (ent-idents (ent-key data-schema)))

(defn get-reference-idents
  "Retrieves all idents with type = ref from given data-schema coll. Returns vector of strings."
  [coll]
  (->> (filterv #(= :ref (:type %)) coll)
       (ent-idents)))

(def data-references
  "Returns hash of entity as key + vector of ref idents on the entity.
   Inside the vector, a hash with a key that represents the attribute name on the ref,
   and value is the ref that it references e.g. {:site [{'location' [:location 'geohash']}
   Entities without refs will return :ent-name []"
  (->> (mapv #(get-reference-idents (% data-schema)) data-entities)
       (zipmap data-entities)))

(defn required-entity-attrs [ent m]
  (->> (ent m)
       (filter #(= false (:optional %)))
       (mapv #(keyword (name ent) (:ident %)))))

(defn lookup-attr-schema-entry
  "Sets the ref-ent as the namespaced keyword if it exists, otherwise returns ent + ident in 2d vector
  (fn :test2 {:ident 'example2'});;=> [:example2 ':test2/example2']"
  [e m]
  (if-let [[ref-ent ref-ident] (:ref-ent m)]
    [(keyword ref-ident) (keyword (name ref-ent) ref-ident)]
    [(keyword (:ident m)) (keyword (name e) (:ident m))]))

(defn set-attr-kv
  "Conjoins schema entry to result hash"
  [results e m]
  (conj results (lookup-attr-schema-entry e m)))

(defn get-namespaced-attrs
  "Provides interface from given symbol to the default namespaced schema attribute as defined in data-schema."
  ([ent] (get-namespaced-attrs ent data-schema))
  ([ent m]
   (->> (ent m)
        (reduce #(set-attr-kv %1 ent %2) {}))))


(defn lookup-db-mapping
  "Returns a hash input attr as key and value as db attr key. e.g. {:firstName :user/firstName}"
  [m data-keys]
  (into {} (map #(vector % (% m))) data-keys))

(defn set-db-id
  ([m] (set-db-id m :db.part/user))
  ([m part] (assoc m :db/id (d/tempid part))))

(defn set-transaction-attrs
  "Expects a map with ent + unique id for lookup + other attributes
  Renames given keys with mapped db keys. Also adds db/id with temp id
  e.g. {:ent :user :recordId 1 :VIP? true} ;;=> (:user/recordId 1 :user/VIP? true :db/id #db/id[db.part/user -100]}"
  [{:keys [ent] :as input-map}]
  (let [attr-map (get-namespaced-attrs ent)
        data-map (dissoc input-map :ent)]
    (->> (lookup-db-mapping attr-map (keys data-map))
         (set/rename-keys data-map)
         (set-db-id))))

(defn generate-datomic-partition
  "Takes a name keyword and returns a transact-able Datomic partition vector with a single map"
  [name-key]
  [{:db/id                 (d/tempid :db.part/db)
    :db/ident              name-key
    :db.install/_partition :db.part/db}])

(defn supported-datomic-types []
  ["string" "boolean" "keyword" "long" "bigint" "float"
   "double" "bigdec" "ref" "instant" "uuid" "uri" "byte"])
