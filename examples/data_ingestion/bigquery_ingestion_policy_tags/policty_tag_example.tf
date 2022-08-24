resource "google_data_catalog_taxonomy" "data_access_control" {
  provider = google-beta
  region = "us"
  display_name =  "Data Access Control"
  description = "A collection of policy tags for Data Lakehouse Access Control"
}

resource "google_data_catalog_policy_tag" "high" {
  provider = google-beta
  taxonomy = google_data_catalog_taxonomy.data_access_control.id
  display_name = "high"
  description = "A policy tag normally associated with high security items"
}

resource "google_data_catalog_policy_tag" "medium" {
  provider = google-beta
  taxonomy = google_data_catalog_taxonomy.data_access_control.id
  display_name = "medium"
  description = "A policy tag normally associated with medium security items"
}

resource "google_data_catalog_policy_tag" "low" {
  provider = google-beta
  taxonomy = google_data_catalog_taxonomy.data_access_control.id
  display_name = "low"
  description = "A policy tag normally associated with low security items"
}