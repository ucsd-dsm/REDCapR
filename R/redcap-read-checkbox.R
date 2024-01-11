redcap_read_checkbox <- function(redcap_uri,
                                 token,
                                 records = NULL,
                                 events = NULL,
                                 fields = NULL,
                                 forms = NULL,
                                 use_batches = FALSE,
                                 blank_for_gray_form_status = TRUE,
                                 blank_for_untouched_checkbox = TRUE,
                                 ...) {
  #   __________________________________________________________________________
  #   load data                                                             ####

  #   ..........................................................................
  #   metadata                                                              ####

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


  #   ..........................................................................
  #   col types                                                             ####

  col_types <- REDCapR::redcap_metadata_coltypes(
    redcap_uri,
    token,
    print_col_types_to_console = FALSE
  )

  # only include selected fields/forms
  col_types[[1]] <- col_types[[1]][ds_metadata$field_name]


  #   ..........................................................................
  #   data                                                                  ####

  # TODO: make usable if user does not specify records, events, or fields
  #       I added `use_batches` to circumvent this but in reality, especially
  #       with a project like ABCD one needs to batch things...
  create_batches <- function(x, n) {
    split(x, ceiling(seq_along(x)/n))
  }

  if (use_batches) {
    batch_size <- round(12000 / (length(events) * length(fields)))
    batch_list <- create_batches(records, batch_size)

    message(
      glue::glue(
        "Starting processing of {length(batch_list)} batches...

        "
      )
    )
    ds_eav <- purrr::map2_dfr(
      batch_list,
      names(batch_list),
      ~ {
        message(
          glue::glue(
            "Batch {.y} of {length(batch_list)}..."
          )
        )
        REDCapR:::redcap_read_eav_oneshot(
          redcap_uri,
          token,
          records = .x,
          events  = events,
          fields  = fields,
          forms   = forms
        )$data
      }
    )
  } else {
    ds_eav <- REDCapR:::redcap_read_eav_oneshot(
      redcap_uri,
      token,
      records = records,
      events  = events,
      fields  = fields,
      forms   = forms
    )$data
  }


  #   __________________________________________________________________________
  #   transform data                                                        ####

  ##  ..........................................................................
  ##  possible values                                                       ####

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
        multiple = "all",
        relationship = "many-to-many"
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
          multiple = "all",
          relationship = "many-to-many"
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
          multiple = "all",
          relationship = "many-to-many"
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
          multiple = "all",
          relationship = "many-to-many"
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
        multiple = "all",
        relationship = "many-to-many"
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
        multiple = "all",
        relationship = "many-to-many"
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


  ##  ..........................................................................
  ##  transform data                                                        ####

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
        "1",
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
  .complete_value_for_untouched_forms <- dplyr::if_else(
    blank_for_gray_form_status,
    NA_character_,
    as.character(REDCapR::constant("form_incomplete"))
  )

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
            dplyr::coalesce(value, "0"),
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
            dplyr::coalesce(value, "0"),
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


  ##  ..........................................................................
  ##  reshape data                                                          ####

  # if ID field is in ds_metadata, add all plumbing fields to returned fields
  # if not, do not include any plumbing fields (while not particularly useful,
  # it resembles standard behavior which allows to download data without the
  # subject ID)
  # TODO: REDCapR is about to change this behavior and always include the ID
  #       and plumbing vars: https://ouhscbbmc.github.io/REDCapR/news/index.html#upcoming-changes-in-v120
  #       We should do that as well
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
    )
  # export everything as text...
  # |>
  # readr::type_convert(col_types)

  ds
}
