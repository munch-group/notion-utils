for label in hpa_brain_prot meritxell_spermatid_expr mult_copy primate_ampl_multi gametologs cDEG nDEG hybridDEG chromatin_genes circRNA xi xi_escape xi_uncertain xi_any_evidence expr_mod_xi_copynr pure_hama hum_nean_admix ari_relate_EUR ari_relate_ASIA ari_relate_AFR ari_nonPUR ari_relate_PUR ari_all candidates ech90_regions linAR_all linAR_human linAR_chimp linAR_gorilla linAR_orang accel_reg_simiiformes_br my_primate_codeml reg_sa_pheno sfari_all_conf intel_seiz_lang intelect_disabil xbrain; do
     echo $label
     python src/notion_utils/add_gene_list.py --column 'google lists' --color gray --sheet $label
done

# for label in hpa_brain_prot meritxell_spermatid_expr mult_copy  nDEG xi; do
#      python src/notion_utils/add_gene_list.py --column 'google lists' --color gray --sheet $label
# done