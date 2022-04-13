SELECT *
FROM {{ params.table }} AS staging_table
            FULL OUTER JOIN projection_popgen.genomic_order ON staging_table.partner_barcode = projection_popgen.genomic_order.specimen_id
            