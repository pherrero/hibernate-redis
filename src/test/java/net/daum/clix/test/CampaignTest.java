package net.daum.clix.test;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.transaction.Synchronization;

import net.daum.clix.Campaign;

import org.hibernate.ConnectionReleaseMode;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.LobCreationContext;
import org.hibernate.engine.jdbc.LobCreator;
import org.hibernate.engine.jdbc.internal.TypeInfo;
import org.hibernate.engine.jdbc.spi.ExtractedDatabaseMetaData;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.ResultSetWrapper;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.transaction.spi.IsolationDelegate;
import org.hibernate.engine.transaction.spi.JoinStatus;
import org.hibernate.engine.transaction.spi.LocalStatus;
import org.hibernate.engine.transaction.spi.TransactionCoordinator;
import org.hibernate.engine.transaction.spi.TransactionFactory;
import org.hibernate.engine.transaction.spi.TransactionImplementor;
import org.hibernate.metamodel.source.MetadataImplementor;
import org.hibernate.service.Service;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.classloading.spi.ClassLoaderService;
import org.hibernate.service.internal.SessionFactoryServiceRegistryImpl;
import org.hibernate.service.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.service.spi.ServiceBinding;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.SessionFactoryServiceRegistryFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.internal.runners.JUnit4ClassRunner;
import org.junit.runner.RunWith;

/**
 * User: jtlee
 * Date: 8/10/12
 * Time: 4:29 PM
 */
@RunWith(JUnit4ClassRunner.class)
public class CampaignTest {

    private static SessionFactory sessionFactory;

  	private static Dialect dialect = new Dialect() { };
	
	private static ExtractedDatabaseMetaData extractedDatabaseMetaData = new ExtractedDatabaseMetaData() {

		@Override
		public boolean supportsScrollableResults() {
			return true;
		}

		@Override
		public boolean supportsGetGeneratedKeys() {
			return false;
		}

		@Override
		public boolean supportsBatchUpdates() {
			return false;
		}

		@Override
		public boolean supportsDataDefinitionInTransaction() {
			return true;
		}

		@Override
		public boolean doesDataDefinitionCauseTransactionCommit() {
			return true;
		}

		@Override
		public Set<String> getExtraKeywords() {
			return Collections.emptySet();
		}

		@Override
		public SQLStateType getSqlStateType() {
			return SQLStateType.SQL99;
		}

		@Override
		public boolean doesLobLocatorUpdateCopy() {
			return false;
		}

		@Override
		public String getConnectionSchemaName() {
			return "";
		}

		@Override
		public String getConnectionCatalogName() {
			return "";
		}

		@Override
		public LinkedHashSet<TypeInfo> getTypeInfoSet() {
			return null;
		} };
	
	private static JdbcServices jdbcServices = new JdbcServices() {

		private static final long serialVersionUID = 1L;

		@Override
		public ConnectionProvider getConnectionProvider() {
			return null;
		}

		@Override
		public Dialect getDialect() {
			return dialect;
		}

		@Override
		public SqlStatementLogger getSqlStatementLogger() {
			return null;
		}

		@Override
		public SqlExceptionHelper getSqlExceptionHelper() {
			return null;
		}

		@Override
		public ExtractedDatabaseMetaData getExtractedMetaDataSupport() {
			return extractedDatabaseMetaData;
		}

		@Override
		public LobCreator getLobCreator(
				LobCreationContext lobCreationContext) {
			return null;
		}

		@Override
		public ResultSetWrapper getResultSetWrapper() {
			return null;
		}};

	private static TransactionFactory<TransactionImplementor> transactionFactory = new TransactionFactory<TransactionImplementor>()  {

		private static final long serialVersionUID = 1L;

		@Override
		public TransactionImplementor createTransaction(
				TransactionCoordinator coordinator) {
			return new TransactionImplementor() {

				@Override
				public boolean isInitiator() {
					return false;
				}

				@Override
				public void begin() {
				}

				@Override
				public void commit() {
				}

				@Override
				public void rollback() {
				}

				@Override
				public LocalStatus getLocalStatus() {
					return LocalStatus.COMMITTED;
				}

				@Override
				public boolean isActive() {
					return false;
				}

				@Override
				public boolean isParticipating() {
					return false;
				}

				@Override
				public boolean wasCommitted() {
					return true;
				}

				@Override
				public boolean wasRolledBack() {
					return false;
				}

				@Override
				public void registerSynchronization(
						Synchronization synchronization)
						throws HibernateException {
					
				}

				@Override
				public void setTimeout(int seconds) {
				}

				@Override
				public int getTimeout() {
					return 0;
				}

				@Override
				public IsolationDelegate createIsolationDelegate() {
					return null;
				}

				@Override
				public JoinStatus getJoinStatus() {
					return null;
				}

				@Override
				public void markForJoin() {
				}

				@Override
				public void join() {
				}

				@Override
				public void resetJoinStatus() {
				}

				@Override
				public void markRollbackOnly() {
				}

				@Override
				public void invalidate() {
				} };
		}

		@Override
		public boolean canBeDriver() {
			return false;
		}

		@Override
		public boolean compatibleWithJtaSynchronization() {
			return false;
		}

		@Override
		public boolean isJoinableJtaTransaction(
				TransactionCoordinator transactionCoordinator,
				TransactionImplementor transaction) {
			return false;
		}

		@Override
		public ConnectionReleaseMode getDefaultReleaseMode() {
			return ConnectionReleaseMode.AFTER_STATEMENT;
		} };
	
	private static ClassLoaderService classLoaderService = new ClassLoaderService() {

		private static final long serialVersionUID = 1L;

		@SuppressWarnings("unchecked")
		@Override
		public <T> Class<T> classForName(String className) {
			try {
				return (Class<T>) getClass().getClassLoader().loadClass(className);
			} catch (ClassNotFoundException e) {
				return null;
			}
		}

		@Override
		public URL locateResource(String name) {
			return getClass().getClassLoader().getResource(name);
		}

		@Override
		public InputStream locateResourceStream(String name) {
			return getClass().getClassLoader().getResourceAsStream(name);
		}

		@Override
		public List<URL> locateResources(String name) {
			List<URL> resources = new ArrayList<URL>();
			try {
				getClass().getClassLoader().getResources(name);
			} catch (IOException e) {
			}
			return resources;
		}

		@Override
		public <S> LinkedHashSet<S> loadJavaServices(
				Class<S> serviceContract) {
			return null;
		} };
	
	private static SessionFactoryServiceRegistryFactory sessionFactoryServiceRegistryFactory = 
			new SessionFactoryServiceRegistryFactory() {

				private static final long serialVersionUID = 1L;

				@Override
				public SessionFactoryServiceRegistryImpl buildServiceRegistry(
						SessionFactoryImplementor sessionFactory,
						Configuration configuration) {
					return new SessionFactoryServiceRegistryImpl(serviceRegistry, sessionFactory, configuration);
				}

				@Override
				public SessionFactoryServiceRegistryImpl buildServiceRegistry(
						SessionFactoryImplementor sessionFactory,
						MetadataImplementor metadata) {
					return new SessionFactoryServiceRegistryImpl(serviceRegistry, sessionFactory, metadata);
				} };
			
	private static ServiceRegistryImplementor serviceRegistry = new ServiceRegistryImplementor() {

		@Override
		public ServiceRegistry getParentServiceRegistry() {
			return null;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <R extends Service> R getService(Class<R> serviceRole) {
			if (serviceRole.equals(JdbcServices.class)) {
				return (R) jdbcServices;
			}
			if (serviceRole.equals(TransactionFactory.class)) {
				return (R) transactionFactory;
			}
			if (serviceRole.equals(ClassLoaderService.class)) {
				return (R) classLoaderService;
			}
			if (serviceRole.equals(SessionFactoryServiceRegistryFactory.class)) {
				return (R) sessionFactoryServiceRegistryFactory;
			}
			return null;
		}

		@Override
		public <R extends Service> ServiceBinding<R> locateServiceBinding(Class<R> serviceRole) {
			R service = getService(serviceRole);
			ServiceBinding<R> serviceBinding = new ServiceBinding<R>(null, serviceRole, service);
			return serviceBinding;
		}

		@Override
		public void destroy() {
			// TODO Auto-generated method stub
			
		} };

    
    @Test
    public void testSave() {

        Session session = sessionFactory.getCurrentSession();

        Transaction tx = session.beginTransaction();

        Campaign campaign = new Campaign();
        campaign.setName("campaign");
        campaign.setBudget(1000L);
        Long id = (Long) session.save(campaign);
        
        System.out.println("Generated ID: " + id);
        tx.commit();

    }

    @BeforeClass
    public static void setUp() {
  			
        sessionFactory = new Configuration().configure().buildSessionFactory(serviceRegistry);

        Session session = sessionFactory.getCurrentSession();

        Transaction tx = session.beginTransaction();

        Campaign campaign = new Campaign();
        campaign.setName("campaign2");
        campaign.setBudget(1000L);
        Long id = (Long) session.save(campaign);
        
        System.out.println("Generated ID: " + id);
        tx.commit();
    }

    @Test
    public void testSelect() {

        Session session = sessionFactory.getCurrentSession();

        Transaction tx = session.beginTransaction();

        Campaign campaign = (Campaign) session.get(Campaign.class, 1L);

        tx.commit();

        assertNotNull(campaign);

    }

    @Test
    public void testQueryCache(){
        Session  session = sessionFactory.getCurrentSession();

        Transaction tx = session.beginTransaction();

        Criteria criteria = session.createCriteria(Campaign.class);
        criteria.add(Restrictions.like("name", "campaign%"));
        criteria.addOrder(Order.asc("name"));
        criteria.setFirstResult(0);
        criteria.setMaxResults(10);
        criteria.setCacheable(true);
//        criteria.setCacheRegion("@Sorted_queryCache");
        
        @SuppressWarnings("unchecked")
		List<Campaign> campaigns = criteria.list();

        tx.commit();

        assertFalse("It should not be empty",campaigns.isEmpty());
    }

    @Test
    public void testCachedQuery(){
        Session  session = sessionFactory.getCurrentSession();

        Transaction tx = session.beginTransaction();

        Criteria criteria = session.createCriteria(Campaign.class);
        criteria.add(Restrictions.like("name", "campaign%"));
        criteria.addOrder(Order.asc("name"));
        criteria.setFirstResult(0);
        criteria.setMaxResults(10);
        criteria.setCacheable(true);
//        criteria.setCacheRegion("@Sorted_queryCache");
        
        @SuppressWarnings("unchecked")
		List<Campaign> campaigns = criteria.list();

        tx.commit();

        assertFalse("It should not be empty",campaigns.isEmpty());

    }

    @Test
    public void testEvictCachedQuery(){

        Session  session = sessionFactory.getCurrentSession();

        Transaction tx = session.beginTransaction();

        Campaign campaign = (Campaign) session.get(Campaign.class, 1L);
        campaign.setName("kampaign100");
        session.saveOrUpdate(campaign);

        Criteria criteria = session.createCriteria(Campaign.class);
        criteria.add(Restrictions.like("name", "campaign%"));
        criteria.addOrder(Order.asc("name"));
        criteria.setFirstResult(0);
        criteria.setMaxResults(10);
        criteria.setCacheable(true);
//        criteria.setCacheRegion("@Sorted_queryCache");
        
        @SuppressWarnings("unchecked")
		List<Campaign> campaigns = criteria.list();

        tx.rollback();

        assertFalse("It should not be empty",campaigns.isEmpty());
    }

    @AfterClass
    public static void tearDown(){
//        JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
//        Jedis jedis = pool.getResource();
//        jedis.flushDB();
    }

}
