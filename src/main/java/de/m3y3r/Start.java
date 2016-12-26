package de.m3y3r;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import bitronix.tm.BitronixTransactionManager;
import bitronix.tm.TransactionManagerServices;
import bitronix.tm.recovery.RecoveryException;
import bitronix.tm.resource.ResourceRegistrar;
import bitronix.tm.resource.jdbc.PoolingDataSource;

/*
 * see XA/open spec, http://pubs.opengroup.org/onlinepubs/009680699/toc.pdf
 * 
 */
public class Start implements Runnable {

	public static void main(String... args) {
		new Start().run();
	}

	@Override
	public void run() {
		BitronixTransactionManager tm = TransactionManagerServices.getTransactionManager();

		try {

			// create and register an XAresource
			PoolingDataSource pds = new PoolingDataSource();
			{
				pds.setClassName("org.apache.derby.jdbc.EmbeddedXADataSource");
				pds.setUniqueName("derby");
				pds.setMaxPoolSize(5);
				Properties driverProperties = new Properties();
				driverProperties.setProperty("databaseName", "myDB");
				driverProperties.setProperty("createDatabase", "create");
				pds.setDriverProperties(driverProperties);
				ResourceRegistrar.register(pds);
			}

			// doesn't work without a JTA transaction, as the btm PoolingDataSource automatically tries to enlist the resource to the current tx
			// but there seems to be a property to override this behaviour
//			try (Connection con = pds.getConnection()) {
//				con.createStatement().execute("create table glass (id integer not null primary key, name varchar(32))");
//			}

			tm.begin();
			Transaction tx = tm.getTransaction();
			try (Connection con = pds.getConnection()) {
				// FINDOUT: is autocommit relevant on the connection?!
//				con.setAutoCommit(true);
				con.createStatement().execute("select 1 from SYSIBM.SYSDUMMY1");
				// okay, when enlisted it is forbidden, to set autocommit to true -> "autocommit is not allowed on a resource enlisted in a global transaction"
//				con.setAutoCommit(true);
				con.createStatement().execute("select 'test' from SYSIBM.SYSDUMMY1");
			}
			tm.commit();
		} catch (NotSupportedException | SystemException | SecurityException | IllegalStateException | RollbackException | HeuristicMixedException | HeuristicRollbackException | RecoveryException | SQLException e) {
			e.printStackTrace();
			try {
				tm.rollback();
			} catch (IllegalStateException | SecurityException | SystemException e1) {
				e1.printStackTrace();
			}
		} finally {
			tm.shutdown();
		}
	}
}
