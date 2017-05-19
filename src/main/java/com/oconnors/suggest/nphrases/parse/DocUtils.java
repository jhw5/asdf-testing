package com.oconnors.suggest.nphrases.parse;

public class DocUtils {

	/**takes the xml tree output by the stanford parser and fixes the xml
	 *
	 * TAGGING RULES:
	 * 		all punctuation tags are replaced by a tag of the format <P_[punctuation name]>
	 * 			e.g. <.> -> <P_PERIOD>
	 * 		except parentheses and other brackets where the normalized tags are replaced
	 * 			by the same tag without the dashes
	 * 			e.g. <-LRB-><[/></-LRB-> -> <LRB>[</LRB>
	 *
	 *		all double quotes, including smart quotes, use tag <P_QUOTE>
	 *			also parses &apos;&apos; as double quote
	 *
	 * 		all terminal tags are replaced by text
	 * 			e.g. <UH><hello/></UH> -> <UH>hello</UH>
	 * 
	 * 		penn tags containing punctuation are also adjusted: <PRP$> -> <PRP_POS>, <WP$> -> <WP_POS>
	 * 
	 * 		new tags are: P_PERIOD, P_COMMA, P_SEMI, P_COLON, P_QUOTE, P_POUND,
	 * 			 P_DOLLAR, RRB, LRB, WP_POS, PRP_POS 
	 * 			note: pretty sure P_SEMI is unused as the parser puts semicolons in colon tags.
	 * 				however ; is still a penn treebank tag so check for it just in case
	 * 			also: currently only P_DOLLAR is used by name by the nphrase builder; other tag names can be changed freely
	 */
	public static String fixXML (String xmlTree){

		xmlTree = xmlTree.replaceAll("<(.*)/>", "$1");
		xmlTree = xmlTree.replaceAll("<(/?)\\.>", "<$1P_PERIOD>");
		xmlTree = xmlTree.replaceAll("<(/?)\\,>", "<$1P_COMMA>");
		xmlTree = xmlTree.replaceAll("<(/?)\\;>", "<$1P_SEMI>");
		xmlTree = xmlTree.replaceAll("<(/?)\\:>", "<$1P_COLON>");
		xmlTree = xmlTree.replaceAll("<(/?)\\\">", "<$1P_QUOTE>");
		xmlTree = xmlTree.replaceAll("<(/?)``>", "<$1P_QUOTE>");
		xmlTree = xmlTree.replaceAll("<(/?)&apos;&apos;>", "<$1P_QUOTE>");
		xmlTree = xmlTree.replaceAll("<(/?)\\#>", "<$1P_POUND>");
		xmlTree = xmlTree.replaceAll("<(/?)\\$>", "<$1P_DOLLAR>");
		xmlTree = xmlTree.replaceAll("<(/?)-(.*)->", "<$1$2>");
		xmlTree = xmlTree.replaceAll("<(/?)(.*)\\$>", "<$1$2_POS>");

		return xmlTree;

	}


}
