(ns user
  (:require [auto-cpo.core :as core]))


(def opts {:options {:config "dev-config.edn"}})

(comment
  (core/update-config! (get-in opts [:options :config]) core/db)

  (core/update-excluded-runs! core/db)

  (core/update-excluded-libraries! core/db)
)
