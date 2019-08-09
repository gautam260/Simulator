import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RowChaining {
	private Connection oracon = null;
	
	void run(int a) {
		
	}
	
	
	void rowMigration() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			System.out.println("Dropping Classic Migration Table if it exists");
			try {
				String sql = "drop table CLASSIC_MIGRATION";
				Statement stmt = oraCon.createStatement();
				stmt.execute(sql);
				stmt.close();
			}
			catch (Exception E) {
				E.printStackTrace();
			}
			System.out.println("Creating Classic Migration Table");
			String SQL = "create table Classic_Migration(t1 number primary key, t2 varchar2(3000), t3 varchar2(2000), t4 varchar2(3000))";
			Statement stmt = oraCon.createStatement();
			stmt.execute(SQL);
			PreparedStatement pstmt = oraCon.prepareStatement("insert into Classic_Migration(t1,t2,t3,t4) values (?,?,?,?)");
			int i = 0;
			System.out.println("Loading Classic Migration Table with Data");
			while (i < 100000) {
				pstmt.setInt(1, oraSequence.nextVal());
				pstmt.setString(2, OraRandom.randomString(1000));
				pstmt.setString(3, OraRandom.randomString(1000));
				pstmt.setString(4, OraRandom.randomString(1000));
				pstmt.addBatch();
				if (i%10000 == 0) {
					pstmt.executeBatch();
				}
				i++;
			}
			pstmt.executeBatch();
			pstmt.close();
			pstmt = oraCon.prepareStatement("update Classic_Migration set t4=? where t1=?");
			i = 0;
			System.out.println("Simulating Migration");
			while (i < 40000) {
				pstmt.setString(1,OraRandom.randomString(3000) );
				pstmt.setInt(2, i);
				pstmt.executeUpdate();
				i++;
			}
			System.out.println("Rows Migrations simulation done.");
			pstmt.close();
			stmt.close();
			oraCon.close();
			ExecutorService asd = Executors.newFixedThreadPool(5);
			i = 0;
			while (i < 5) {
				asd.submit(new MigrationLoader());
				i++;
				
			}
			asd.shutdown();
			System.out.println("Started Classic Load ");
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	class MigrationLoader implements Runnable{
		public void run() {
			try {
				Connection con = DBConnection.getOraConn();
				Statement Stmt = con.createStatement();
				ResultSet rs;
				int i = 0;
				while (i < 100000000) {
					rs = Stmt.executeQuery("select count(t4) from Classic_Migration");
					while(rs.next()) {
						rs.getInt(1);
					}
					i++;
				}
				Stmt.close();
				con.close();
				
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	void classicChaining() {
		try {
			Connection oraCon = DBConnection.getOraConn();
			System.out.println("Dropping Classic Chaining Table if it exists");
			try {
				String sql = "drop table Classic_Chaining";
				Statement stmt = oracon.createStatement();
				stmt.execute(sql);
				stmt.close();
			}
			catch (Exception E) {
			}
			System.out.println("Creating Classic Chaining Table");
			
			String SQL = "create table Classic_Chaining(t1 number primary key, t2 varchar2(3000), t3 varchar2(2000), t4 varchar2(3000))";
			Statement stmt = oraCon.createStatement();
			stmt.execute(SQL);
			PreparedStatement pstmt = oraCon.prepareStatement("insert into classic_chaining(t1,t2,t3,t4) values (?,?,?,?)");
			int i = 0;
			System.out.println("Loading Classic Chaining Table with Data");
			while (i < 100000) {
				pstmt.setInt(1, oraSequence.nextVal());
				pstmt.setString(2, OraRandom.randomString(3000));
				pstmt.setString(3, OraRandom.randomString(2000));
				pstmt.setString(4, OraRandom.randomString(3000));
				pstmt.addBatch();
				if (i%10000 == 0) {
					pstmt.executeBatch();
				}
				i++;
			}
			pstmt.executeBatch();
			pstmt.close();
			stmt.close();
			oraCon.close();
			ExecutorService asd = Executors.newFixedThreadPool(10);
			i = 0;
			while (i < 5) {
				asd.submit(new classicLoader());
				asd.submit(new classicLoader2());
				i++;
				
			}
			asd.shutdown();
			System.out.println("Started Classic Load ");
			
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	
	class classicLoader implements Runnable{
		public void run() {
			try {
				Connection con = DBConnection.getOraConn();
				PreparedStatement pstmt = con.prepareStatement("select t4 from Classic_Chaining where t1 = ?");
				int i = 0;
				ResultSet rs;
				while (i < 100000000) {
					pstmt.setInt(1, OraRandom.randomUniformInt(100000));
					rs = pstmt.executeQuery();
					while(rs.next()) {
						rs.getString(1);
					}
					i++;
				}
				pstmt.close();
				con.close();
				
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	class classicLoader2 implements Runnable{
		public void run() {
			try {
				Connection con = DBConnection.getOraConn();
				PreparedStatement pstmt = con.prepareStatement("select t2 from Classic_Chaining where t1 = ?");
				int i = 0;
				ResultSet rs;
				while (i < 100000000) {
					pstmt.setInt(1, OraRandom.randomUniformInt(100000));
					rs = pstmt.executeQuery();
					while(rs.next()) {
						rs.getString(1);
					}
					i++;
				}
				pstmt.close();
				con.close();
				
			}
			catch(Exception E) {
				E.printStackTrace();
			}
		}
	}
	
	
	void Intrablock() {
		try {
			oracon = DBConnection.getOraConn();
			System.out.println("Dropping Intrablock Chaining Table if it exists");
			try {
				String sql = "drop table IntraBlock_Chaining";
				Statement stmt = oracon.createStatement();
				stmt.execute(sql);
				stmt.close();
			}
			catch (Exception E) {
				
			}
			System.out.println("Creating Intrablock Chaining Table");
			String SQL = "create table IntraBlock_Chaining(t1 number primary key," + 
					"t2 number," + 
					"t3 number," + 
					"t4 number," + 
					"t5 number," + 
					"t6 number," + 
					"t7 number," + 
					"t8 number," + 
					"t9 number," + 
					"t10 number," + 
					"t11 number," + 
					"t12 number," + 
					"t13 number," + 
					"t14 number," + 
					"t15 number," + 
					"t16 number," + 
					"t17 number," + 
					"t18 number," + 
					"t19 number," + 
					"t20 number," + 
					"t21 number," + 
					"t22 number," + 
					"t23 number," + 
					"t24 number," + 
					"t25 number," + 
					"t26 number," + 
					"t27 number," + 
					"t28 number," + 
					"t29 number," + 
					"t30 number," + 
					"t31 number," + 
					"t32 number," + 
					"t33 number," + 
					"t34 number," + 
					"t35 number," + 
					"t36 number," + 
					"t37 number," + 
					"t38 number," + 
					"t39 number," + 
					"t40 number," + 
					"t41 number," + 
					"t42 number," + 
					"t43 number," + 
					"t44 number," + 
					"t45 number," + 
					"t46 number," + 
					"t47 number," + 
					"t48 number," + 
					"t49 number," + 
					"t50 number," + 
					"t51 number," + 
					"t52 number," + 
					"t53 number," + 
					"t54 number," + 
					"t55 number," + 
					"t56 number," + 
					"t57 number," + 
					"t58 number," + 
					"t59 number," + 
					"t60 number," + 
					"t61 number," + 
					"t62 number," + 
					"t63 number," + 
					"t64 number," + 
					"t65 number," + 
					"t66 number," + 
					"t67 number," + 
					"t68 number," + 
					"t69 number," + 
					"t70 number," + 
					"t71 number," + 
					"t72 number," + 
					"t73 number," + 
					"t74 number," + 
					"t75 number," + 
					"t76 number," + 
					"t77 number," + 
					"t78 number," + 
					"t79 number," + 
					"t80 number," + 
					"t81 number," + 
					"t82 number," + 
					"t83 number," + 
					"t84 number," + 
					"t85 number," + 
					"t86 number," + 
					"t87 number," + 
					"t88 number," + 
					"t89 number," + 
					"t90 number," + 
					"t91 number," + 
					"t92 number," + 
					"t93 number," + 
					"t94 number," + 
					"t95 number," + 
					"t96 number," + 
					"t97 number," + 
					"t98 number," + 
					"t99 number," + 
					"t100 number," + 
					"t101 number," + 
					"t102 number," + 
					"t103 number," + 
					"t104 number," + 
					"t105 number," + 
					"t106 number," + 
					"t107 number," + 
					"t108 number," + 
					"t109 number," + 
					"t110 number," + 
					"t111 number," + 
					"t112 number," + 
					"t113 number," + 
					"t114 number," + 
					"t115 number," + 
					"t116 number," + 
					"t117 number," + 
					"t118 number," + 
					"t119 number," + 
					"t120 number," + 
					"t121 number," + 
					"t122 number," + 
					"t123 number," + 
					"t124 number," + 
					"t125 number," + 
					"t126 number," + 
					"t127 number," + 
					"t128 number," + 
					"t129 number," + 
					"t130 number," + 
					"t131 number," + 
					"t132 number," + 
					"t133 number," + 
					"t134 number," + 
					"t135 number," + 
					"t136 number," + 
					"t137 number," + 
					"t138 number," + 
					"t139 number," + 
					"t140 number," + 
					"t141 number," + 
					"t142 number," + 
					"t143 number," + 
					"t144 number," + 
					"t145 number," + 
					"t146 number," + 
					"t147 number," + 
					"t148 number," + 
					"t149 number," + 
					"t150 number," + 
					"t151 number," + 
					"t152 number," + 
					"t153 number," + 
					"t154 number," + 
					"t155 number," + 
					"t156 number," + 
					"t157 number," + 
					"t158 number," + 
					"t159 number," + 
					"t160 number," + 
					"t161 number," + 
					"t162 number," + 
					"t163 number," + 
					"t164 number," + 
					"t165 number," + 
					"t166 number," + 
					"t167 number," + 
					"t168 number," + 
					"t169 number," + 
					"t170 number," + 
					"t171 number," + 
					"t172 number," + 
					"t173 number," + 
					"t174 number," + 
					"t175 number," + 
					"t176 number," + 
					"t177 number," + 
					"t178 number," + 
					"t179 number," + 
					"t180 number," + 
					"t181 number," + 
					"t182 number," + 
					"t183 number," + 
					"t184 number," + 
					"t185 number," + 
					"t186 number," + 
					"t187 number," + 
					"t188 number," + 
					"t189 number," + 
					"t190 number," + 
					"t191 number," + 
					"t192 number," + 
					"t193 number," + 
					"t194 number," + 
					"t195 number," + 
					"t196 number," + 
					"t197 number," + 
					"t198 number," + 
					"t199 number," + 
					"t200 number," + 
					"t201 number," + 
					"t202 number," + 
					"t203 number," + 
					"t204 number," + 
					"t205 number," + 
					"t206 number," + 
					"t207 number," + 
					"t208 number," + 
					"t209 number," + 
					"t210 number," + 
					"t211 number," + 
					"t212 number," + 
					"t213 number," + 
					"t214 number," + 
					"t215 number," + 
					"t216 number," + 
					"t217 number," + 
					"t218 number," + 
					"t219 number," + 
					"t220 number," + 
					"t221 number," + 
					"t222 number," + 
					"t223 number," + 
					"t224 number," + 
					"t225 number," + 
					"t226 number," + 
					"t227 number," + 
					"t228 number," + 
					"t229 number," + 
					"t230 number," + 
					"t231 number," + 
					"t232 number," + 
					"t233 number," + 
					"t234 number," + 
					"t235 number," + 
					"t236 number," + 
					"t237 number," + 
					"t238 number," + 
					"t239 number," + 
					"t240 number," + 
					"t241 number," + 
					"t242 number," + 
					"t243 number," + 
					"t244 number," + 
					"t245 number," + 
					"t246 number," + 
					"t247 number," + 
					"t248 number," + 
					"t249 number," + 
					"t250 number," + 
					"t251 number," + 
					"t252 number," + 
					"t253 number," + 
					"t254 number," + 
					"t255 number," + 
					"t256 number," + 
					"t257 number," + 
					"t258 number," + 
					"t259 number," + 
					"t260 number," + 
					"t261 number," + 
					"t262 number," + 
					"t263 number," + 
					"t264 number," + 
					"t265 number)";
			Statement stmt = oracon.createStatement();
			stmt.execute(SQL);
			System.out.println("Loading data into Intrablock Chaining Table");
			PreparedStatement pstmt = oracon.prepareStatement("insert into IntraBlock_Chaining(t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14,t15,t16,t17,t18,t19,t20,t21,t22,t23,t24,t25,t26,t27,t28,t29,t30,t31,t32,t33,t34,t35,t36,t37,t38,t39,t40,t41,t42,t43,t44,t45,t46,t47,t48,t49,t50,t51,t52,t53,t54,t55,t56,t57,t58,t59,t60,t61,t62,t63,t64,t65,t66,t67,t68,t69,t70,t71,t72,t73,t74,t75,t76,t77,t78,t79,t80,t81,t82,t83,t84,t85,t86,t87,t88,t89,t90,t91,t92,t93,t94,t95,t96,t97,t98,t99,t100,t101,t102,t103,t104,t105,t106,t107,t108,t109,t110,t111,t112,t113,t114,t115,t116,t117,t118,t119,t120,t121,t122,t123,t124,t125,t126,t127,t128,t129,t130,t131,t132,t133,t134,t135,t136,t137,t138,t139,t140,t141,t142,t143,t144,t145,t146,t147,t148,t149,t150,t151,t152,t153,t154,t155,t156,t157,t158,t159,t160,t161,t162,t163,t164,t165,t166,t167,t168,t169,t170,t171,t172,t173,t174,t175,t176,t177,t178,t179,t180,t181,t182,t183,t184,t185,t186,t187,t188,t189,t190,t191,t192,t193,t194,t195,t196,t197,t198,t199,t200,t201,t202,t203,t204,t205,t206,t207,t208,t209,t210,t211,t212,t213,t214,t215,t216,t217,t218,t219,t220,t221,t222,t223,t224,t225,t226,t227,t228,t229,t230,t231,t232,t233,t234,t235,t236,t237,t238,t239,t240,t241,t242,t243,t244,t245,t246,t247,t248,t249,t250,t251,t252,t253,t254,t255,t256,t257,t258,t259,t260,t261,t262,t263,t264,t265) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			int i = 0 ;
			while (i < 100000) {
				pstmt.setInt(1, oraSequence.nextVal());
				pstmt.setInt(2, OraRandom.randomUniformInt(100));
				pstmt.setInt(3, OraRandom.randomUniformInt(100));
				pstmt.setInt(4, OraRandom.randomUniformInt(100));
				pstmt.setInt(5, OraRandom.randomUniformInt(100));
				pstmt.setInt(6, OraRandom.randomUniformInt(100));
				pstmt.setInt(7, OraRandom.randomUniformInt(100));
				pstmt.setInt(8, OraRandom.randomUniformInt(100));
				pstmt.setInt(9, OraRandom.randomUniformInt(100));
				pstmt.setInt(10, OraRandom.randomUniformInt(100));
				pstmt.setInt(11, OraRandom.randomUniformInt(100));
				pstmt.setInt(12, OraRandom.randomUniformInt(100));
				pstmt.setInt(13, OraRandom.randomUniformInt(100));
				pstmt.setInt(14, OraRandom.randomUniformInt(100));
				pstmt.setInt(15, OraRandom.randomUniformInt(100));
				pstmt.setInt(16, OraRandom.randomUniformInt(100));
				pstmt.setInt(17, OraRandom.randomUniformInt(100));
				pstmt.setInt(18, OraRandom.randomUniformInt(100));
				pstmt.setInt(19, OraRandom.randomUniformInt(100));
				pstmt.setInt(20, OraRandom.randomUniformInt(100));
				pstmt.setInt(21, OraRandom.randomUniformInt(100));
				pstmt.setInt(22, OraRandom.randomUniformInt(100));
				pstmt.setInt(23, OraRandom.randomUniformInt(100));
				pstmt.setInt(24, OraRandom.randomUniformInt(100));
				pstmt.setInt(25, OraRandom.randomUniformInt(100));
				pstmt.setInt(26, OraRandom.randomUniformInt(100));
				pstmt.setInt(27, OraRandom.randomUniformInt(100));
				pstmt.setInt(28, OraRandom.randomUniformInt(100));
				pstmt.setInt(29, OraRandom.randomUniformInt(100));
				pstmt.setInt(30, OraRandom.randomUniformInt(100));
				pstmt.setInt(31, OraRandom.randomUniformInt(100));
				pstmt.setInt(32, OraRandom.randomUniformInt(100));
				pstmt.setInt(33, OraRandom.randomUniformInt(100));
				pstmt.setInt(34, OraRandom.randomUniformInt(100));
				pstmt.setInt(35, OraRandom.randomUniformInt(100));
				pstmt.setInt(36, OraRandom.randomUniformInt(100));
				pstmt.setInt(37, OraRandom.randomUniformInt(100));
				pstmt.setInt(38, OraRandom.randomUniformInt(100));
				pstmt.setInt(39, OraRandom.randomUniformInt(100));
				pstmt.setInt(40, OraRandom.randomUniformInt(100));
				pstmt.setInt(41, OraRandom.randomUniformInt(100));
				pstmt.setInt(42, OraRandom.randomUniformInt(100));
				pstmt.setInt(43, OraRandom.randomUniformInt(100));
				pstmt.setInt(44, OraRandom.randomUniformInt(100));
				pstmt.setInt(45, OraRandom.randomUniformInt(100));
				pstmt.setInt(46, OraRandom.randomUniformInt(100));
				pstmt.setInt(47, OraRandom.randomUniformInt(100));
				pstmt.setInt(48, OraRandom.randomUniformInt(100));
				pstmt.setInt(49, OraRandom.randomUniformInt(100));
				pstmt.setInt(50, OraRandom.randomUniformInt(100));
				pstmt.setInt(51, OraRandom.randomUniformInt(100));
				pstmt.setInt(52, OraRandom.randomUniformInt(100));
				pstmt.setInt(53, OraRandom.randomUniformInt(100));
				pstmt.setInt(54, OraRandom.randomUniformInt(100));
				pstmt.setInt(55, OraRandom.randomUniformInt(100));
				pstmt.setInt(56, OraRandom.randomUniformInt(100));
				pstmt.setInt(57, OraRandom.randomUniformInt(100));
				pstmt.setInt(58, OraRandom.randomUniformInt(100));
				pstmt.setInt(59, OraRandom.randomUniformInt(100));
				pstmt.setInt(60, OraRandom.randomUniformInt(100));
				pstmt.setInt(61, OraRandom.randomUniformInt(100));
				pstmt.setInt(62, OraRandom.randomUniformInt(100));
				pstmt.setInt(63, OraRandom.randomUniformInt(100));
				pstmt.setInt(64, OraRandom.randomUniformInt(100));
				pstmt.setInt(65, OraRandom.randomUniformInt(100));
				pstmt.setInt(66, OraRandom.randomUniformInt(100));
				pstmt.setInt(67, OraRandom.randomUniformInt(100));
				pstmt.setInt(68, OraRandom.randomUniformInt(100));
				pstmt.setInt(69, OraRandom.randomUniformInt(100));
				pstmt.setInt(70, OraRandom.randomUniformInt(100));
				pstmt.setInt(71, OraRandom.randomUniformInt(100));
				pstmt.setInt(72, OraRandom.randomUniformInt(100));
				pstmt.setInt(73, OraRandom.randomUniformInt(100));
				pstmt.setInt(74, OraRandom.randomUniformInt(100));
				pstmt.setInt(75, OraRandom.randomUniformInt(100));
				pstmt.setInt(76, OraRandom.randomUniformInt(100));
				pstmt.setInt(77, OraRandom.randomUniformInt(100));
				pstmt.setInt(78, OraRandom.randomUniformInt(100));
				pstmt.setInt(79, OraRandom.randomUniformInt(100));
				pstmt.setInt(80, OraRandom.randomUniformInt(100));
				pstmt.setInt(81, OraRandom.randomUniformInt(100));
				pstmt.setInt(82, OraRandom.randomUniformInt(100));
				pstmt.setInt(83, OraRandom.randomUniformInt(100));
				pstmt.setInt(84, OraRandom.randomUniformInt(100));
				pstmt.setInt(85, OraRandom.randomUniformInt(100));
				pstmt.setInt(86, OraRandom.randomUniformInt(100));
				pstmt.setInt(87, OraRandom.randomUniformInt(100));
				pstmt.setInt(88, OraRandom.randomUniformInt(100));
				pstmt.setInt(89, OraRandom.randomUniformInt(100));
				pstmt.setInt(90, OraRandom.randomUniformInt(100));
				pstmt.setInt(91, OraRandom.randomUniformInt(100));
				pstmt.setInt(92, OraRandom.randomUniformInt(100));
				pstmt.setInt(93, OraRandom.randomUniformInt(100));
				pstmt.setInt(94, OraRandom.randomUniformInt(100));
				pstmt.setInt(95, OraRandom.randomUniformInt(100));
				pstmt.setInt(96, OraRandom.randomUniformInt(100));
				pstmt.setInt(97, OraRandom.randomUniformInt(100));
				pstmt.setInt(98, OraRandom.randomUniformInt(100));
				pstmt.setInt(99, OraRandom.randomUniformInt(100));
				pstmt.setInt(100, OraRandom.randomUniformInt(100));
				pstmt.setInt(101, OraRandom.randomUniformInt(100));
				pstmt.setInt(102, OraRandom.randomUniformInt(100));
				pstmt.setInt(103, OraRandom.randomUniformInt(100));
				pstmt.setInt(104, OraRandom.randomUniformInt(100));
				pstmt.setInt(105, OraRandom.randomUniformInt(100));
				pstmt.setInt(106, OraRandom.randomUniformInt(100));
				pstmt.setInt(107, OraRandom.randomUniformInt(100));
				pstmt.setInt(108, OraRandom.randomUniformInt(100));
				pstmt.setInt(109, OraRandom.randomUniformInt(100));
				pstmt.setInt(110, OraRandom.randomUniformInt(100));
				pstmt.setInt(111, OraRandom.randomUniformInt(100));
				pstmt.setInt(112, OraRandom.randomUniformInt(100));
				pstmt.setInt(113, OraRandom.randomUniformInt(100));
				pstmt.setInt(114, OraRandom.randomUniformInt(100));
				pstmt.setInt(115, OraRandom.randomUniformInt(100));
				pstmt.setInt(116, OraRandom.randomUniformInt(100));
				pstmt.setInt(117, OraRandom.randomUniformInt(100));
				pstmt.setInt(118, OraRandom.randomUniformInt(100));
				pstmt.setInt(119, OraRandom.randomUniformInt(100));
				pstmt.setInt(120, OraRandom.randomUniformInt(100));
				pstmt.setInt(121, OraRandom.randomUniformInt(100));
				pstmt.setInt(122, OraRandom.randomUniformInt(100));
				pstmt.setInt(123, OraRandom.randomUniformInt(100));
				pstmt.setInt(124, OraRandom.randomUniformInt(100));
				pstmt.setInt(125, OraRandom.randomUniformInt(100));
				pstmt.setInt(126, OraRandom.randomUniformInt(100));
				pstmt.setInt(127, OraRandom.randomUniformInt(100));
				pstmt.setInt(128, OraRandom.randomUniformInt(100));
				pstmt.setInt(129, OraRandom.randomUniformInt(100));
				pstmt.setInt(130, OraRandom.randomUniformInt(100));
				pstmt.setInt(131, OraRandom.randomUniformInt(100));
				pstmt.setInt(132, OraRandom.randomUniformInt(100));
				pstmt.setInt(133, OraRandom.randomUniformInt(100));
				pstmt.setInt(134, OraRandom.randomUniformInt(100));
				pstmt.setInt(135, OraRandom.randomUniformInt(100));
				pstmt.setInt(136, OraRandom.randomUniformInt(100));
				pstmt.setInt(137, OraRandom.randomUniformInt(100));
				pstmt.setInt(138, OraRandom.randomUniformInt(100));
				pstmt.setInt(139, OraRandom.randomUniformInt(100));
				pstmt.setInt(140, OraRandom.randomUniformInt(100));
				pstmt.setInt(141, OraRandom.randomUniformInt(100));
				pstmt.setInt(142, OraRandom.randomUniformInt(100));
				pstmt.setInt(143, OraRandom.randomUniformInt(100));
				pstmt.setInt(144, OraRandom.randomUniformInt(100));
				pstmt.setInt(145, OraRandom.randomUniformInt(100));
				pstmt.setInt(146, OraRandom.randomUniformInt(100));
				pstmt.setInt(147, OraRandom.randomUniformInt(100));
				pstmt.setInt(148, OraRandom.randomUniformInt(100));
				pstmt.setInt(149, OraRandom.randomUniformInt(100));
				pstmt.setInt(150, OraRandom.randomUniformInt(100));
				pstmt.setInt(151, OraRandom.randomUniformInt(100));
				pstmt.setInt(152, OraRandom.randomUniformInt(100));
				pstmt.setInt(153, OraRandom.randomUniformInt(100));
				pstmt.setInt(154, OraRandom.randomUniformInt(100));
				pstmt.setInt(155, OraRandom.randomUniformInt(100));
				pstmt.setInt(156, OraRandom.randomUniformInt(100));
				pstmt.setInt(157, OraRandom.randomUniformInt(100));
				pstmt.setInt(158, OraRandom.randomUniformInt(100));
				pstmt.setInt(159, OraRandom.randomUniformInt(100));
				pstmt.setInt(160, OraRandom.randomUniformInt(100));
				pstmt.setInt(161, OraRandom.randomUniformInt(100));
				pstmt.setInt(162, OraRandom.randomUniformInt(100));
				pstmt.setInt(163, OraRandom.randomUniformInt(100));
				pstmt.setInt(164, OraRandom.randomUniformInt(100));
				pstmt.setInt(165, OraRandom.randomUniformInt(100));
				pstmt.setInt(166, OraRandom.randomUniformInt(100));
				pstmt.setInt(167, OraRandom.randomUniformInt(100));
				pstmt.setInt(168, OraRandom.randomUniformInt(100));
				pstmt.setInt(169, OraRandom.randomUniformInt(100));
				pstmt.setInt(170, OraRandom.randomUniformInt(100));
				pstmt.setInt(171, OraRandom.randomUniformInt(100));
				pstmt.setInt(172, OraRandom.randomUniformInt(100));
				pstmt.setInt(173, OraRandom.randomUniformInt(100));
				pstmt.setInt(174, OraRandom.randomUniformInt(100));
				pstmt.setInt(175, OraRandom.randomUniformInt(100));
				pstmt.setInt(176, OraRandom.randomUniformInt(100));
				pstmt.setInt(177, OraRandom.randomUniformInt(100));
				pstmt.setInt(178, OraRandom.randomUniformInt(100));
				pstmt.setInt(179, OraRandom.randomUniformInt(100));
				pstmt.setInt(180, OraRandom.randomUniformInt(100));
				pstmt.setInt(181, OraRandom.randomUniformInt(100));
				pstmt.setInt(182, OraRandom.randomUniformInt(100));
				pstmt.setInt(183, OraRandom.randomUniformInt(100));
				pstmt.setInt(184, OraRandom.randomUniformInt(100));
				pstmt.setInt(185, OraRandom.randomUniformInt(100));
				pstmt.setInt(186, OraRandom.randomUniformInt(100));
				pstmt.setInt(187, OraRandom.randomUniformInt(100));
				pstmt.setInt(188, OraRandom.randomUniformInt(100));
				pstmt.setInt(189, OraRandom.randomUniformInt(100));
				pstmt.setInt(190, OraRandom.randomUniformInt(100));
				pstmt.setInt(191, OraRandom.randomUniformInt(100));
				pstmt.setInt(192, OraRandom.randomUniformInt(100));
				pstmt.setInt(193, OraRandom.randomUniformInt(100));
				pstmt.setInt(194, OraRandom.randomUniformInt(100));
				pstmt.setInt(195, OraRandom.randomUniformInt(100));
				pstmt.setInt(196, OraRandom.randomUniformInt(100));
				pstmt.setInt(197, OraRandom.randomUniformInt(100));
				pstmt.setInt(198, OraRandom.randomUniformInt(100));
				pstmt.setInt(199, OraRandom.randomUniformInt(100));
				pstmt.setInt(200, OraRandom.randomUniformInt(100));
				pstmt.setInt(201, OraRandom.randomUniformInt(100));
				pstmt.setInt(202, OraRandom.randomUniformInt(100));
				pstmt.setInt(203, OraRandom.randomUniformInt(100));
				pstmt.setInt(204, OraRandom.randomUniformInt(100));
				pstmt.setInt(205, OraRandom.randomUniformInt(100));
				pstmt.setInt(206, OraRandom.randomUniformInt(100));
				pstmt.setInt(207, OraRandom.randomUniformInt(100));
				pstmt.setInt(208, OraRandom.randomUniformInt(100));
				pstmt.setInt(209, OraRandom.randomUniformInt(100));
				pstmt.setInt(210, OraRandom.randomUniformInt(100));
				pstmt.setInt(211, OraRandom.randomUniformInt(100));
				pstmt.setInt(212, OraRandom.randomUniformInt(100));
				pstmt.setInt(213, OraRandom.randomUniformInt(100));
				pstmt.setInt(214, OraRandom.randomUniformInt(100));
				pstmt.setInt(215, OraRandom.randomUniformInt(100));
				pstmt.setInt(216, OraRandom.randomUniformInt(100));
				pstmt.setInt(217, OraRandom.randomUniformInt(100));
				pstmt.setInt(218, OraRandom.randomUniformInt(100));
				pstmt.setInt(219, OraRandom.randomUniformInt(100));
				pstmt.setInt(220, OraRandom.randomUniformInt(100));
				pstmt.setInt(221, OraRandom.randomUniformInt(100));
				pstmt.setInt(222, OraRandom.randomUniformInt(100));
				pstmt.setInt(223, OraRandom.randomUniformInt(100));
				pstmt.setInt(224, OraRandom.randomUniformInt(100));
				pstmt.setInt(225, OraRandom.randomUniformInt(100));
				pstmt.setInt(226, OraRandom.randomUniformInt(100));
				pstmt.setInt(227, OraRandom.randomUniformInt(100));
				pstmt.setInt(228, OraRandom.randomUniformInt(100));
				pstmt.setInt(229, OraRandom.randomUniformInt(100));
				pstmt.setInt(230, OraRandom.randomUniformInt(100));
				pstmt.setInt(231, OraRandom.randomUniformInt(100));
				pstmt.setInt(232, OraRandom.randomUniformInt(100));
				pstmt.setInt(233, OraRandom.randomUniformInt(100));
				pstmt.setInt(234, OraRandom.randomUniformInt(100));
				pstmt.setInt(235, OraRandom.randomUniformInt(100));
				pstmt.setInt(236, OraRandom.randomUniformInt(100));
				pstmt.setInt(237, OraRandom.randomUniformInt(100));
				pstmt.setInt(238, OraRandom.randomUniformInt(100));
				pstmt.setInt(239, OraRandom.randomUniformInt(100));
				pstmt.setInt(240, OraRandom.randomUniformInt(100));
				pstmt.setInt(241, OraRandom.randomUniformInt(100));
				pstmt.setInt(242, OraRandom.randomUniformInt(100));
				pstmt.setInt(243, OraRandom.randomUniformInt(100));
				pstmt.setInt(244, OraRandom.randomUniformInt(100));
				pstmt.setInt(245, OraRandom.randomUniformInt(100));
				pstmt.setInt(246, OraRandom.randomUniformInt(100));
				pstmt.setInt(247, OraRandom.randomUniformInt(100));
				pstmt.setInt(248, OraRandom.randomUniformInt(100));
				pstmt.setInt(249, OraRandom.randomUniformInt(100));
				pstmt.setInt(250, OraRandom.randomUniformInt(100));
				pstmt.setInt(251, OraRandom.randomUniformInt(100));
				pstmt.setInt(252, OraRandom.randomUniformInt(100));
				pstmt.setInt(253, OraRandom.randomUniformInt(100));
				pstmt.setInt(254, OraRandom.randomUniformInt(100));
				pstmt.setInt(255, OraRandom.randomUniformInt(100));
				pstmt.setInt(256, OraRandom.randomUniformInt(100));
				pstmt.setInt(257, OraRandom.randomUniformInt(100));
				pstmt.setInt(258, OraRandom.randomUniformInt(100));
				pstmt.setInt(259, OraRandom.randomUniformInt(100));
				pstmt.setInt(260, OraRandom.randomUniformInt(100));
				pstmt.setInt(261, OraRandom.randomUniformInt(100));
				pstmt.setInt(262, OraRandom.randomUniformInt(100));
				pstmt.setInt(263, OraRandom.randomUniformInt(100));
				pstmt.setInt(264, OraRandom.randomUniformInt(100));
				pstmt.setInt(265, OraRandom.randomUniformInt(100));
				pstmt.addBatch();
				if (i %100000 == 0) {
					pstmt.executeBatch();
					System.out.println("Loaded:" +oraSequence.getval() + "rows");
				}
				i++;
			}
			pstmt.executeBatch();
			pstmt.close();
			stmt.close();
			ExecutorService asd = Executors.newFixedThreadPool(10);
			i = 0;
			while (i < 10) {
				asd.submit(new IntraBlockRunner());
				i++;
				
			}
			asd.shutdown();
			System.out.println("IntraBlock chaining Load Started ");
			oracon.close();
		}
		catch(Exception E) {
			E.printStackTrace();
		}
	}
	class IntraBlockRunner implements Runnable {
		public void run() {
			try {
				Connection con = DBConnection.getOraConn();
				PreparedStatement pstmt = con.prepareStatement("select t264 from IntraBlock_Chaining where t1 < ?");
				int i = 0;
				ResultSet rs;
				while (i < 100000000) {
					pstmt.setInt(1, OraRandom.randomUniformInt(100000));
					rs = pstmt.executeQuery();
					while(rs.next()) {
						rs.getInt(1);
					}
					i++;
				}
				pstmt.close();
				con.close();
				
			}
			catch(Exception E) {
				E.printStackTrace();
			}
			
		}
		
	}

}
