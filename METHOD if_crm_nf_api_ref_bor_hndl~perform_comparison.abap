METHOD if_crm_nf_api_ref_bor_hndl~perform_comparison.
  INCLUDE: crm_srcl_con.

  TYPES: BEGIN OF ts_filter_check,
                tabname	      TYPE crmd_nf_filters-tabname,
                fname	        TYPE crmd_nf_filters-fname,
                log_criterion	TYPE crmt_nf_log_criteria,
                rv_passed_or  TYPE abap_bool,
          END OF ts_filter_check.
  TYPES: tt_filter_check TYPE TABLE OF ts_filter_check.

  CONSTANTS:
    lc_refobj_segment TYPE string VALUE 'SRV_REFOBJ',
    lc_subject_segment TYPE string VALUE 'SRV_SUBJECT',
    lc_srvclk_segment TYPE string VALUE 'SRCL_H'.

  DATA: lt_filter_check TYPE tt_filter_check,
        lv_passed_local TYPE abap_bool.

  DATA:
    lv_where    TYPE string,
    lv_fld_val  TYPE string,
    lv_fld_name TYPE string,
    lv_proc_type TYPE crmc_proc_type-process_type.

  DATA:
    lv_trh      TYPE crmt_boolean,
    lt_selopts  TYPE crmt_report_selopt_ta,
    lt_srclid   TYPE TABLE OF crmt_or_text,
    lv_lines    TYPE i,
    lv_srcl     TYPE crmt_boolean,
    lv_srcl_id  TYPE crmt_boolean.

  DATA: lt_filters_check_local TYPE cl_crm_nf_core_shm_root=>gtt_filters.

  FIELD-SYMBOLS:
    <ft_segment>  TYPE ANY TABLE,

    <fs_filters>  LIKE LINE OF it_filters,
    <fs_seg_key>  LIKE LINE OF gt_segment_keys,
    <fs_sel_opts> TYPE crmt_report_selopt,

    <fv_guid>     TYPE any,
    <fv_os_guid>  TYPE any,
    <fs_segment>  TYPE any,
    <fv_fld_val>  TYPE any,

    <fs_filter_check> TYPE ts_filter_check.

** get the header guid, means limited to 1order transactions

  ASSIGN COMPONENT 'ORDERADM_H' OF STRUCTURE is_bdoc TO <ft_segment>.

* <ft_segment> is type ANY TABLE, hence not possible to use READ TABLE
  LOOP AT <ft_segment> ASSIGNING <fs_segment>.
    ASSIGN COMPONENT 'ORDERADM_H_GUID' OF STRUCTURE <fs_segment> TO <fv_guid>.
    gv_ref_guid = <fv_guid>.
    EXIT.
  ENDLOOP.

  "service clocks preprocessing
  READ TABLE it_filters TRANSPORTING NO FIELDS WITH KEY tabname = lc_srvclk_segment fname = 'SRCL_TRH_LVL'.
  IF sy-subrc EQ 0.
    LOOP AT it_filters ASSIGNING <fs_filters> WHERE tabname EQ lc_srvclk_segment AND fname EQ 'ID'.
      "combination of threshold level and clock id needs to be checked
      READ TABLE <fs_filters>-sel_opts ASSIGNING <fs_sel_opts> INDEX 1.
      INSERT <fs_sel_opts>-low INTO TABLE lt_srclid.
    ENDLOOP.
  ENDIF.

  DESCRIBE TABLE lt_srclid LINES lv_lines.
  IF lv_lines GT 0.
    lv_srcl = abap_true. "we have more than 1 srcl to check the trh level against
  ENDIF.

  LOOP AT it_filters ASSIGNING <fs_filters>.

    AT NEW tabname.

* reset all vars
      UNASSIGN: <ft_segment>, <fs_seg_key>, <fv_fld_val>.
      CLEAR: lv_where, lv_fld_val, lv_fld_name.
      lv_passed_local = abap_false.

** if either the segment or segment key cant be found, then fail the entire check

* special handling needed for these 2 segments as they are nested in SRV_OSSET
      IF <fs_filters>-tabname EQ lc_subject_segment OR
         <fs_filters>-tabname EQ lc_refobj_segment.

        ASSIGN COMPONENT 'SRV_OSSET' OF STRUCTURE is_bdoc TO <ft_segment>.
        READ TABLE <ft_segment> ASSIGNING <fs_segment> WITH KEY "#EC CI_ANYSEQ
          ('REF_KIND') = 'A'. "header
        ASSIGN COMPONENT 'GUID' OF STRUCTURE <fs_segment> TO <fv_os_guid>.

        lv_where = 'GUID_REF = <FV_OS_GUID>'.

      ELSE.

        READ TABLE gt_segment_keys ASSIGNING <fs_seg_key>
          WITH TABLE KEY segment = <fs_filters>-tabname.
        IF sy-subrc NE 0. RETURN. ENDIF.

        CONCATENATE <fs_seg_key>-guid_key '=' '<FV_GUID>' INTO lv_where
          SEPARATED BY space.

        "additional handling for the service clock ID to get the service clock ID(s) we are interested in
        IF <fs_filters>-tabname EQ lc_srvclk_segment AND <fs_filters>-fname EQ 'ID'.
          lt_selopts = <fs_filters>-sel_opts.
        ENDIF.

      ENDIF.

      ASSIGN COMPONENT <fs_filters>-tabname OF STRUCTURE is_bdoc
        TO <ft_segment>.
      IF sy-subrc NE 0. RETURN. ENDIF.

    ENDAT.

* if any 1 filter (OR logic) fails, then the entire check fail
    lv_passed_local = abap_false.

* limit to only header-related records
    LOOP AT <ft_segment> ASSIGNING <fs_segment> WHERE (lv_where).

* if log key value is supplied, check the log key value
      IF <fs_filters>-log_criterion IS NOT INITIAL.
        ASSIGN COMPONENT <fs_seg_key>-log_key OF STRUCTURE <fs_segment> TO <fv_fld_val>.
        CHECK sy-subrc EQ 0 AND <fs_filters>-log_criterion EQ <fv_fld_val>.
      ENDIF.

      IF <fs_filters>-fname NE 'SRCL_TRH_LVL'.
        "service clocks segments are named differently, no added handling needed since we're looking for a field value that is initial or not later
        ASSIGN COMPONENT <fs_filters>-fname OF STRUCTURE <fs_segment> TO <fv_fld_val>.
        CHECK sy-subrc EQ 0.
      ENDIF.

* special handling for statuses and categories
      CASE <fs_filters>-tabname.
        WHEN 'STATUS'.
          IF <fs_filters>-fname = 'STATUS'.
            lv_fld_name = 'USER_STAT_PROC'.
          ENDIF.
        WHEN lc_subject_segment.
          IF <fs_filters>-fname = 'CAT_ID'.
            lv_fld_name = 'ASP_ID'.
          ENDIF.
        WHEN lc_srvclk_segment.
          IF <fs_filters>-fname = 'ID'.
            lv_srcl_id = abap_true.
            CLEAR lv_fld_name.
          ELSEIF <fs_filters>-fname = 'SRCL_TRH_LVL'.
            "special handling for threshold levels
            lv_fld_name = 'SRCL_TRH'.
            lv_trh = abap_true.
          ENDIF.
      ENDCASE.

** check that the field is valid and meets the filter criteria

* code for special handling
      IF lv_fld_name IS NOT INITIAL.
        IF lv_trh EQ abap_true.
          "we are looking for thresholds, which requie checking against the threshold and a status
          ASSIGN COMPONENT 'STATUS' OF STRUCTURE <fs_segment> TO <fv_fld_val>.
          CHECK sy-subrc EQ 0.

          IF <fv_fld_val> NE gc_service_clock_status-stopped AND
              <fv_fld_val> NE gc_service_clock_status-cancelled.
            "now we check for the threshold
            ASSIGN COMPONENT 'THRESHOLD_NOTIF' OF STRUCTURE <fs_segment> TO <fv_fld_val>.
            CHECK sy-subrc EQ 0.
            lv_fld_val = <fv_fld_val>.
            READ TABLE <fs_filters>-sel_opts ASSIGNING <fs_sel_opts> INDEX 1.

            "get the value and check if it's maintained, indicating it was reached
            ASSIGN COMPONENT <fs_sel_opts>-low OF STRUCTURE <fs_segment> TO <fv_fld_val>.
            CHECK sy-subrc EQ 0.

            IF <fv_fld_val> IS NOT INITIAL AND lv_fld_val EQ <fs_sel_opts>-low.
              "now make sure we are looking at the correct service clock segment line
              ASSIGN COMPONENT 'ID' OF STRUCTURE <fs_segment> TO <fv_fld_val>.
              CHECK sy-subrc EQ 0.
              IF lv_srcl EQ abap_true.
                DELETE lt_srclid WHERE table_line EQ <fv_fld_val>. "this segment is one of those we want to check against
                DESCRIBE TABLE lt_srclid LINES lv_lines.
                IF lv_lines EQ 0.
                  lv_passed_local = abap_true.
                ENDIF.
              ELSE.
                lv_passed_local = abap_true.
              ENDIF.
            ENDIF.
          ENDIF.

        ELSE.
* construct the field to check against the pre-defined criteria
          lv_fld_val = <fv_fld_val>.
          ASSIGN COMPONENT lv_fld_name OF STRUCTURE <fs_segment> TO <fv_fld_val>.
          CHECK sy-subrc EQ 0.
          IF <fs_filters>-tabname = 'STATUS'.
            SELECT process_type FROM crmc_proc_type INTO lv_proc_type WHERE user_stat_proc = <fv_fld_val>.ENDSELECT.
            CONCATENATE lv_proc_type lv_fld_val INTO lv_fld_val SEPARATED BY ':'.
          ELSE.
            CONCATENATE <fv_fld_val> lv_fld_val INTO lv_fld_val SEPARATED BY ':'.
          ENDIF.

          FIND FIRST OCCURRENCE OF lv_fld_val IN TABLE <fs_filters>-sel_opts.
          IF sy-subrc = 0.
            lv_passed_local = abap_true.
          ENDIF.
          EXIT.

        ENDIF.

* code for general handling
      ELSEIF <fv_fld_val> IN <fs_filters>-sel_opts.
        lv_passed_local = abap_true.
        IF lv_srcl_id EQ abap_true.
          "must check status still
          ASSIGN COMPONENT 'STATUS' OF STRUCTURE <fs_segment> TO <fv_fld_val>.
          CHECK sy-subrc EQ 0.

          IF <fv_fld_val> EQ gc_service_clock_status-stopped OR
              <fv_fld_val> EQ gc_service_clock_status-cancelled.
            lv_passed_local = abap_false.
            EXIT.
          ENDIF.
        ENDIF.
        EXIT.
      ENDIF.
    ENDLOOP.

** if any 1 check fail, then the entire check fail
*    CHECK rv_passed EQ abap_false.
*    RETURN.

* in one criteria (tabname/fname/log_criterion) conditions are connected with OR logic (!!)
    UNASSIGN <fs_filter_check>.
    READ TABLE lt_filter_check ASSIGNING <fs_filter_check>
      WITH KEY tabname = <fs_filters>-tabname
               fname = <fs_filters>-fname
               log_criterion = <fs_filters>-log_criterion.
    IF sy-subrc EQ 0.
      IF <fs_filter_check>-rv_passed_or EQ abap_false AND lv_passed_local EQ abap_true.
        <fs_filter_check>-rv_passed_or = lv_passed_local.
      ENDIF.
    ELSE.
      APPEND INITIAL LINE TO lt_filter_check ASSIGNING <fs_filter_check>.
      MOVE-CORRESPONDING <fs_filters> TO <fs_filter_check>.
      <fs_filter_check>-rv_passed_or = lv_passed_local.
    ENDIF.
  ENDLOOP.

* all checks have passed
  READ TABLE lt_filter_check TRANSPORTING NO FIELDS
    WITH KEY rv_passed_or = abap_false.
  IF sy-subrc EQ 0.
    rv_passed = abap_false.
  ELSE.
    rv_passed = abap_true.
  ENDIF.
ENDMETHOD.
