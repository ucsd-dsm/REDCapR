rm(list = ls(all = TRUE)) #Clear the memory of variables from previous run. This is not called by knitr, because it's above the first chunk.

# ---- load-sources ------------------------------------------------------------

# ---- load-packages -----------------------------------------------------------
library("magrittr")
requireNamespace("dplyr")
requireNamespace("readr")
requireNamespace("testit")

# ---- declare-globals ---------------------------------------------------------
# as projects are only containing test data, okay to commit URL and tokens
# to Github in private repo
# remove if we ever submit a PR to the main project
redcap_uri <- "https://redcap.hbcd.cmig-ucsd.org/api/"

# token      <- "53320377CD34F60392870F5F6C20953B" # longitudinal & repeating
# token      <- "778FAF179F3DBEDBC1A3CA1CAB2B36EF" # repeating
# token      <- "831D17A42D03BC448A7B1A08CDE19AD9" # longitudinal
token      <- "B77827EB144D9B7D74F01E932C9F0B29" # neither

fields  <- NULL
# fields   <- c(
#   # "record_id",
#   "form_1_check_001"
# )
forms   <- NULL
# forms   <- c(
#   "form_1",
#   # "form_2",
#   "form_3"
#   # "form_4"
# )
events  <- NULL
# events  <- c(
#   "event_1_arm_1"
#   # "event_3_arm_1"
# )
records <- NULL

blank_for_gray_form_status <- TRUE
.complete_value_for_untouched_forms <- dplyr::if_else(
  blank_for_gray_form_status,
  NA_character_,
  as.character(REDCapR::constant("form_incomplete"))
)

# NOTE: I think that an additional function parameter to specify how to handle
#   untouched checkbox items might make sense. But we can also make it dependent
#   on `blank_for_gray_form_status` if you prefer that. I just think some people
#   might want to make different selections for either.
blank_for_untouched_checkbox <- TRUE

# ---- load-data ---------------------------------------------------------------
# View(REDCapR::redcap_variables(redcap_uri, token)$data)
system.time({
  meta <- REDCapR:::redcap_metadata_internal(
    redcap_uri,
    token
  )

  ds_metadata          <- meta$d_variable
  ds_metadata_plumbing <- ds_metadata |>
    dplyr::filter(
      plumbing
    )

  # only include selected fields/forms
  if (!is.null(fields) & !is.null(forms)) {
    ds_metadata <- ds_metadata |>
      dplyr::filter(
        field_name_base %in% fields | form_name %in% forms
      )
  } else if (!is.null(fields)) {
    ds_metadata <- ds_metadata |>
      dplyr::filter(
        field_name_base %in% fields
      )
  } else if (!is.null(forms)) {
    ds_metadata <- ds_metadata |>
      dplyr::filter(
        form_name %in% forms
      )
  }

  col_types <- REDCapR::redcap_metadata_coltypes(
    redcap_uri,
    token,
    print_col_types_to_console = FALSE
  )

  # only include selected fields/forms
  col_types[[1]] <- col_types[[1]][ds_metadata$field_name]

  ds_eav <- REDCapR:::redcap_read_eav_oneshot(
    redcap_uri,
    token,
    fields               = fields,
    forms                = forms,
    events               = events,
    records              = records,
    export_survey_fields = TRUE
  )$data
})

# ---- tweak-data --------------------------------------------------------------
# NOTE: while "standard" exports always include `redcap_repeat_...` columns if
#   the project has at least one repeating instrument, eav exports only include
#   them IF
#   - forms (or fields from forms) are included that are repeating
#   - AND the event(s) for which they are set to repeating is/are included
#   - AND there is actually data for the included participants
#
#   Thus we have to make this determination dependent on the data and cannot use
#   meta$repeating
.repeating    <- "redcap_repeat_instrument" %in% names(ds_eav)
.longitudinal <- meta$longitudinal

.fields_plumbing <- "record"

if (.longitudinal) {
  .fields_plumbing <- c(
    .fields_plumbing,
    "redcap_event_name"
  )

  # get longitudinal instrument metadata
  instruments_events <- redcap_event_instruments(
    redcap_uri,
    token
  )$data |>
    dplyr::select(
      -arm_num
    ) |>
    dplyr::rename(
      redcap_event_name  = unique_event_name,
      form_name          = form
    )

  # only include selected forms/events
  instruments_events <- instruments_events |>
    dplyr::filter(
      form_name %in% ds_metadata$form_name
    )
  if (!is.null(events)) {
    instruments_events <- instruments_events |>
      dplyr::filter(
        redcap_event_name %in% events
      )
  }
}

if (.repeating) {
  .fields_plumbing <- c(
    .fields_plumbing,
    "redcap_repeat_instrument",
    "redcap_repeat_instance"
  )

  # get repeating instrument metadata
  instruments_repeating <- redcap_repeating(
    redcap_uri,
    token
  )$data |>
    dplyr::select(
      -custom_form_label
    )

  # only include selected forms/events
  instruments_repeating <- instruments_repeating |>
    dplyr::filter(
      form_name %in% ds_metadata$form_name
    )
  if (!is.null(events)) {
    instruments_repeating <- instruments_repeating |>
      dplyr::filter(
        event_name %in% events
      )
  }
}

# create dataframe of possible values
if (.longitudinal & .repeating) {
  .fields_to_cross <- ds_metadata |>
    dplyr::filter(
      !plumbing
    ) |>
    dplyr::select(
      field_name,
      form_name
    ) |>
    dplyr::left_join(
      instruments_events,
      by       = "form_name",
      multiple = "all"
    )

  .fields_to_cross_repeating <- .fields_to_cross |>
    dplyr::right_join(
      instruments_repeating,
      by = c(
        "form_name",
        "redcap_event_name" = "event_name"
      )
    )

  .fields_to_cross_not_repeating <- .fields_to_cross |>
    dplyr::anti_join(
      instruments_repeating,
      by = c(
        "form_name",
        "redcap_event_name" = "event_name"
      )
    ) |>
    dplyr::select(
      -form_name
    )

  ds_eav_expand <- ds_eav |>
    tidyr::expand(
      tidyr::nesting(
        record,
        redcap_event_name,
        redcap_repeat_instrument,
        redcap_repeat_instance
      )
    )

  ds_eav_possible <- dplyr::bind_rows(
    # not repeating
    ds_eav_expand |>
      dplyr::filter(
        is.na(redcap_repeat_instrument)
      ) |>
      dplyr::left_join(
        .fields_to_cross_not_repeating,
        by = c(
          "redcap_event_name"
        ),
        multiple = "all"
      ) |>
      # eliminate empty events (i.e., rows without field names).
      # this happens if an event only contains repeating forms as eav exports
      # always include a record ID field for each event
      dplyr::filter(
        !is.na(field_name)
      ),
    # repeating
    ds_eav_expand |>
      dplyr::filter(
        !is.na(redcap_repeat_instrument)
      ) |>
      dplyr::left_join(
        .fields_to_cross_repeating,
        by = c(
          "redcap_event_name",
          "redcap_repeat_instrument" = "form_name"
        ),
        multiple = "all"
      )
  )
} else if (.repeating) {
  .fields_to_cross <- ds_metadata |>
    dplyr::filter(
      !plumbing
    ) |>
    dplyr::select(
      field_name,
      form_name
    )

  .fields_to_cross_repeating <- .fields_to_cross |>
    dplyr::filter(
      form_name %in% instruments_repeating$form_name
    )

  .fields_to_cross_not_repeating <- .fields_to_cross |>
    dplyr::filter(
      !form_name %in% instruments_repeating$form_name
    ) |>
    dplyr::select(
      -form_name
    )

  ds_eav_expand <- ds_eav |>
    tidyr::expand(
      tidyr::nesting(
        record,
        redcap_repeat_instrument,
        redcap_repeat_instance
      )
    )

  ds_eav_possible <- dplyr::bind_rows(
    # not repeating
    ds_eav_expand |>
      dplyr::filter(
        is.na(redcap_repeat_instrument)
      ) |>
      tidyr::crossing(
        .fields_to_cross_not_repeating
      ),
    # repeating
    ds_eav_expand |>
      dplyr::filter(
        !is.na(redcap_repeat_instrument)
      ) |>
      dplyr::left_join(
        .fields_to_cross_repeating,
        by = c(
          "redcap_repeat_instrument" = "form_name"
        ),
        multiple = "all"
      )
  )
} else if (.longitudinal) {
  .fields_to_cross <- ds_metadata |>
    dplyr::filter(
      !plumbing
    ) |>
    dplyr::select(
      field_name,
      form_name
    ) |>
    dplyr::left_join(
      instruments_events,
      by       = "form_name",
      multiple = "all"
    ) |>
    dplyr::select(
      -form_name
    )

  ds_eav_possible <- ds_eav |>
    tidyr::expand(
      tidyr::nesting(
        record,
        redcap_event_name
      )
    ) |>
    dplyr::left_join(
      .fields_to_cross,
      by = c(
        "redcap_event_name"
      ),
      multiple = "all"
    )
} else {
  .fields_to_cross <- ds_metadata |>
    dplyr::filter(
      !plumbing
    ) |>
    dplyr::select(
      field_name,
    )

  ds_eav_possible <- ds_eav |>
    tidyr::expand(
      tidyr::nesting(
        record
      ),
      tidyr::crossing(
        .fields_to_cross
      )
    )
}

# transform eav data
ds_eav_2 <- ds_eav |>
  # include field type column
  dplyr::rename(field_name_base = field_name) |>
  dplyr::left_join(
    ds_metadata |>
      dplyr::distinct(.data$field_name_base, .data$field_type),
    by = "field_name_base"
  ) |>

  # convert values from eav format to standard format
  dplyr::mutate(
    checkbox   = !is.na(.data$field_type) & (.data$field_type == "checkbox"),
    field_name = dplyr::if_else(
      .data$checkbox,
      paste0(.data$field_name_base , "___", .data$value),
      .data$field_name_base
    ),
    value      = dplyr::if_else(
      .data$checkbox,
      "TRUE",
      .data$value
    )
  ) |>

  # get rid of helper fields
  dplyr::right_join(ds_eav_possible, by = c(.fields_plumbing, "field_name")) |>
  dplyr::select(-"field_type", -"field_name_base", -"checkbox") |>

  # join new helper fields
  dplyr::left_join(
    ds_metadata |>
      dplyr::select("field_name", "field_name_base", "field_type"),
    by = "field_name"
  )

# coalesce values for NAs depending on user's choice how to handle untouched
# checkbox fields
if (blank_for_untouched_checkbox) {
  ds_eav_2_coalesce <- ds_eav_2 |>
    dplyr::mutate(
      touched = dplyr::if_else(
        !is.na(value),
        1,
        0
      )
    ) |>
    dplyr::group_by(
      dplyr::across(
        dplyr::all_of(
          c(
            .fields_plumbing,
            "field_name_base"
          )
        )
      )
    ) |>
    dplyr::mutate(
      any_touched = sum(touched) > 0
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      value = dplyr::case_when(
        .data$field_type == "checkbox" & .data$any_touched ~
          dplyr::coalesce(value, "FALSE"),
        .data$field_type == "complete" ~
          dplyr::coalesce(value, .complete_value_for_untouched_forms),
        TRUE ~ value
      ),
    ) |>
    dplyr::select(
      -c(
        "field_type",
        "field_name_base",
        "touched",
        "any_touched"
      )
    )
} else {
  ds_eav_2_coalesce <- ds_eav_2 |>
    dplyr::mutate(
      value = dplyr::case_when(
        .data$field_type == "checkbox" ~
          dplyr::coalesce(value, "FALSE"),
        .data$field_type == "complete" ~
          dplyr::coalesce(value, .complete_value_for_untouched_forms),
        TRUE ~ value
      )
    ) |>
    dplyr::select(
      -c(
        "field_type",
        "field_name_base"
      )
    )
}

# if ID field is in ds_metadata, add all plumbing fields to returned fields
# if not, do not include any plumbing fields (while not particularly useful, it
# resembles standard behavior which allows to download data without the subject
# ID)
.fields_to_return <- ds_metadata |>
  dplyr::filter(
    !plumbing
  ) |>
  dplyr::pull(
    field_name
  )

.record_id_name <- meta$d_variable$field_name[1]

if(.record_id_name %in% ds_metadata$field_name) {
  .fields_to_return <- c(
    .fields_plumbing,
    .fields_to_return
  )

  .fields_to_return[1] <- .record_id_name
}

# pivot data into standard format
ds <- ds_eav_2_coalesce |>
  tidyr::pivot_wider(
    id_cols     = !!.fields_plumbing,
    names_from  = "field_name",
    values_from = "value"
  ) |>
  dplyr::rename(
    {{ .record_id_name }} := record
  ) |>
  dplyr::select(
    dplyr::all_of(.fields_to_return)
  ) |>
  readr::type_convert(col_types)


# compare to standard ----------------------------------------------------------

system.time(
  ds_standard <- REDCapR::redcap_read_oneshot(
    redcap_uri,
    token,
    fields                      = fields,
    forms                       = forms,
    events                      = events,
    records                     = records,
    col_types                   = col_types,
    blank_for_gray_form_status  = blank_for_gray_form_status
  )$data
)

testit::assert(ds_metadata$field_name == colnames(ds_standard))
testthat::expect_setequal(ds_metadata$field_name, colnames(ds_standard))

testit::assert(colnames(ds) == colnames(ds_standard))
testthat::expect_setequal(colnames(ds), colnames(ds_standard))

# visually compare standard vs. eav-based export
group_col <- .fields_plumbing
group_col[1] <- .record_id_name

compareDF::compare_df(
  ds,
  ds_standard,
  group_col = group_col
) |>
  compareDF::view_html()


# use the function instead -----------------------------------------------------

ds <- redcap_read_checkbox(
  redcap_uri = redcap_uri,
  token      = token
)

ds_standard <- redcap_read(
  redcap_uri = redcap_uri,
  token      = token
)$data

compareDF::compare_df(
  ds,
  ds_standard,
  group_col = c(
    "record_id"
    # "redcap_event_name",
    # "redcap_repeat_instrument",
    # "redcap_repeat_instance"
  )
) |>
  compareDF::view_html()
