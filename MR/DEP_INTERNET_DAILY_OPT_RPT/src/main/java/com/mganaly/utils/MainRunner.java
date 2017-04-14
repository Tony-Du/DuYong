package com.mganaly.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mganaly.conf.GlobalEv;
import com.mganaly.mrtools.utils.cdmp_format_check_tool;
import com.mganaly.mrtools.utils.ods_to_voms_show_obj_d_tools;
import com.mganaly.mrtools.utils.td_aaa_bill_d_tool;
import com.mganaly.mrtools.utils.td_aaa_tourist_bill_d_tool;
import com.mganaly.mrtools.utils.td_cms_content_attr_d_tool;
import com.mganaly.mrtools.utils.td_cms_content_class_d_tool;
import com.mganaly.mrtools.utils.td_cms_content_d_tool;
import com.mganaly.mrtools.utils.td_icms_drama_sin_rel_tool;
import com.mganaly.mrtools.utils.td_oms_program_d_tool;
import com.mganaly.mrtools.utils.cdmp_filter_comparator_tool;
import com.mganaly.mrtools.utils.map_join_tool;
import com.mganaly.mrtools.utils.mysql_table_submit;
import com.mganaly.mrtools.utils.outer_join_tool;
import com.mganaly.mrtools.utils.prep_seperate_tool;
import com.mganaly.mrtools.utils.merge_tool;
import com.mganaly.mrtools.utils.td_pub_visit_log_d_tool;
import com.mganaly.mrtools.utils.td_pub_visit_log_wd_tourist_d_tool;
import com.mganaly.mrtools.utils.td_userid_usernum_tool;
import com.mganaly.mrtools.utils.inner_join_tool;
import com.mganaly.mrtools.utils.left_join_tool;
import com.mganaly.mrtools.utils.select_tool;
import com.mganaly.mrtools.utils.mr_general_tool;
import com.mganaly.mrtools.utils.tdim_prod_id_tool;

public class MainRunner extends Configured implements Tool {
	
	private static final Log _LOG = LogFactory.getLog(MainRunner.class);

	int exitCode = 0;
	private static MRCmds cmds = new MRCmds();

	private static void prompTips() {		
		cmds.printTips();
	}
	
	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			prompTips();
			return;
		}

		String command = new String();
		
		String[] subArgs;
		if (args[0].equals("-d"))
		{
			_LOG.info("MapReduce running in debug mode...");
			GlobalEv.DEBUG = true;
			
			// Parse command in debug mode
			command = args[1];
			subArgs = new String[args.length - 2];
	
			System.arraycopy(args, 2, subArgs, 0, subArgs.length);
			
		}
		else
		{
			GlobalEv.DEBUG = false;
			
			// Parse command in cluster mode
			command = args[0];
			subArgs = new String[args.length - 1];	
			System.arraycopy(args, 1, subArgs, 0, subArgs.length);
		}

		int exitCode = 0;
		
		
		// Execute commands		
		if (0 == command.compareToIgnoreCase( MRCmds.eType.Cluster.name())) {
			//exitCode = ToolRunner.run(new ClusterData(), subArgs);
		} 
		else if (0 == command.compareToIgnoreCase( 
				MRCmds.eType.td_aaa_bill_d_tool.name())) {
			
				exitCode = ToolRunner.run(new td_aaa_bill_d_tool(), subArgs);
				
		} 
		else if (0 == command.compareToIgnoreCase( 
				MRCmds.eType.td_aaa_tourist_bill_d_tool.name())) {

				exitCode = ToolRunner.run(new td_aaa_tourist_bill_d_tool(), subArgs);
				
		} 
		else if (0 == command.compareToIgnoreCase( 
				MRCmds.eType.td_pub_visit_log_d_tool.name())) {
			
				exitCode = ToolRunner.run(new td_pub_visit_log_d_tool(), subArgs);
				
		} 
		else if (0 == command.compareToIgnoreCase( 
				MRCmds.eType.td_pub_visit_log_wd_tourist_d_tool.name())) {
			
				exitCode = ToolRunner.run(new td_pub_visit_log_wd_tourist_d_tool(), subArgs);	
		}
		else if (0 == command.compareToIgnoreCase( 
						MRCmds.eType.ods_to_voms_show_obj_d_tool.name())) {
			
			exitCode = ToolRunner.run(new ods_to_voms_show_obj_d_tools(), subArgs);
		}
		else if (0 == command.compareToIgnoreCase( 
						MRCmds.eType.td_oms_program_d_tool.name())) {
			
			exitCode = ToolRunner.run(new td_oms_program_d_tool(), subArgs);
		}
		else if (0 == command.compareToIgnoreCase( 
						MRCmds.eType.td_icms_drama_sin_rel_tool.name())) {
			
			exitCode = ToolRunner.run(new td_icms_drama_sin_rel_tool(), subArgs);
			
		}
		else if (0 == command.compareToIgnoreCase( 
						MRCmds.eType.td_cms_content_attr_d_tool.name())) {
			
			exitCode = ToolRunner.run(new td_cms_content_attr_d_tool(), subArgs);
			
		}
		else if (0 == command.compareToIgnoreCase( 
						MRCmds.eType.td_cms_content_class_d_tool.name())) {
			
			exitCode = ToolRunner.run(new td_cms_content_class_d_tool(), subArgs);
		}
		else if (0 == command.compareToIgnoreCase( 
						MRCmds.eType.td_cms_content_d_tool.name())) {
			
			exitCode = ToolRunner.run(new td_cms_content_d_tool(), subArgs);

		}
		else if (0 == command.compareToIgnoreCase( 
						MRCmds.eType.td_userid_usernum_tool.name())) {
			
			exitCode = ToolRunner.run(new td_userid_usernum_tool(), subArgs);
			
		} 
		else if (0 == command.compareToIgnoreCase( 
					MRCmds.eType.cdmp_format_check_tool.name())) {
			
			exitCode = ToolRunner.run(new cdmp_format_check_tool(), subArgs);
			
		}
		else if (0 == command.compareToIgnoreCase( 
						MRCmds.eType.cdmp_filter_comparator_tool.name())) {
			
			exitCode = ToolRunner.run(new cdmp_filter_comparator_tool(), subArgs);

		}
		else if (0 == command.compareToIgnoreCase( 
						MRCmds.eType.prep_seperate_tool.name())) {
			
			exitCode = ToolRunner.run(new prep_seperate_tool(), subArgs);

		} 
		else if (0 == command.compareToIgnoreCase( 
				MRCmds.eType.map_join_tool.name())) {
			
			exitCode = ToolRunner.run(new map_join_tool(), subArgs);
			
		} 
		else if (0 == command.compareToIgnoreCase( 
				MRCmds.eType.merge_tool.name())) {
			
			exitCode = ToolRunner.run(new merge_tool(), subArgs);
			
		}
		else if (0 == command.compareToIgnoreCase( 
				MRCmds.eType.inner_join_tool.name())) {
			
			exitCode = ToolRunner.run(new inner_join_tool(), subArgs);
			
		} 
		else if (0 == command.compareToIgnoreCase( 
				MRCmds.eType.left_join_tool.name())) {
			
			exitCode = ToolRunner.run(new left_join_tool(), subArgs);
			
		} 
		else if (0 == command.compareToIgnoreCase( 
				MRCmds.eType.outer_join_tool.name())) {
			
			exitCode = ToolRunner.run(new outer_join_tool(), subArgs);
			
		} 
		else if (0 == command.compareToIgnoreCase( 
				MRCmds.eType.select_tool.name())) {
			
			exitCode = ToolRunner.run(new select_tool(), subArgs);

		}
		else if (0 == command.compareToIgnoreCase( 
				MRCmds.eType.mr_general_tool.name())) {
			
			exitCode = ToolRunner.run(new mr_general_tool(), subArgs);
			
		}
		else if (0 == command.compareToIgnoreCase( 
				MRCmds.eType.tdim_prod_id_tool.name())) {
			
			exitCode = ToolRunner.run(new tdim_prod_id_tool(), subArgs);
			
		} 
		else if (0 == command.compareToIgnoreCase(
				MRCmds.eType.mysql_table_submit.name())) {

				exitCode = ToolRunner.run(new mysql_table_submit(), subArgs);

		}
		else if (0 == command.compareToIgnoreCase(
				MRCmds.eType.cmd_del_path.name())) {
			
			exitCode = 0;
			_LOG.info("Delete path ");
		}
		
		else {
			_LOG.info("Unknown command: " + command);
			exitCode = ToolRunner.run(new MainRunner(), subArgs);
		}
		
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {

		prompTips();	
		System.exit(1);
		return 0;
	}
}
