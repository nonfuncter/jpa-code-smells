package com.nonfunc.jpa.iterative.examples;

/*
 * #%L
 * em
 * %%
 * Copyright (C) 2016 nonfunc.com
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import static com.airhacks.rulz.em.EntityManagerProvider.persistenceUnit;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.LockModeType;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runners.JUnit4;

import com.airhacks.rulz.em.EntityManagerProvider;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

public class IterativeJpaTest {

	private static Logger logger = Logger.getLogger(IterativeJpaTest.class.getName());
	private static EntityManager entityManager;
	private static EntityTransaction transaction;

	@ClassRule
	public static EntityManagerProvider provider = persistenceUnit("hibernate-dynamic");

	@Before
	public void before() {
		entityManager = provider.em();
		transaction = provider.tx();
		transaction.begin();

		entityManager.flush();
		entityManager.clear();
	}
	
	@After
	public void after() {
		transaction.rollback();
	}

	private void initilizeDb(int size) {
		for (int i = 0; i < size; i++) {
			Foo foo = new Foo();
			foo.setCode(i);
			foo.setDescription(UUID.randomUUID().toString());
			foo.setDescription2(UUID.randomUUID().toString());
			entityManager.persist(foo);	
		}				
		entityManager.flush();
		entityManager.clear();
	}
	
	
	@Test	
	public void testIterativeJpaUsage() {	
		final int size = 20000;
		initilizeDb(size);
		
		//To make sure there is no disadvantage for the first select.
		Query query = entityManager.createQuery("SELECT f FROM Foo f", Foo.class);	
		query.getResultList();
		entityManager.clear();
		
		Stopwatch watch = Stopwatch.createStarted();
		query = entityManager.createQuery("SELECT f FROM Foo f", Foo.class);		
		Foo foo = (Foo) query.getResultList().stream().filter(f -> ((Foo) f).getCode() == 19999).findFirst().get();
		log("Filtering all with Java.",watch.elapsed(TimeUnit.NANOSECONDS));
		Assert.assertNotNull(foo);		
				
		entityManager.clear();
		
		watch.reset();
		watch.start();
		query = entityManager.createQuery("SELECT f FROM Foo f", Foo.class);		
		foo = (Foo) query.getResultList().parallelStream().filter(f -> ((Foo) f).getCode() == 19999).findFirst().get();
		log("Filtering all with Java parallelStream.",watch.elapsed(TimeUnit.NANOSECONDS));
		Assert.assertNotNull(foo);	
		
		entityManager.clear();
		
		watch.reset();
		watch.start();
		query = entityManager.createQuery("SELECT f FROM Foo f WHERE f.code = :code", Foo.class);
		query.setParameter("code", 19999);		
		foo = (Foo) query.getSingleResult();
		log("Select using WHERE clause.",watch.elapsed(TimeUnit.NANOSECONDS));
		Assert.assertNotNull(foo);
	}
	
	@Test
	public void updateEntitiesOfSameType() {
		initilizeDb(20000);
		
		Query query = entityManager.createQuery("SELECT f FROM Foo f WHERE f.code > 10000 AND f.code < 15000", Foo.class);
		
		@SuppressWarnings({ "unused", "unchecked" })
		List<Foo> foos = query.getResultList();
		
		Stopwatch watch = Stopwatch.createStarted();
		for (Foo foo : foos) {
			foo.setDescription("UPDATED");
			entityManager.merge(foo);
			entityManager.flush();
		}		
		log("Updating entities iteratively with flush every time.", watch.elapsed(TimeUnit.NANOSECONDS));
		
		query = entityManager.createQuery("DELETE FROM Foo");
		query.executeUpdate();
		entityManager.clear();
		
		initilizeDb(20000);
		
		query = entityManager.createQuery("SELECT f FROM Foo f WHERE f.code > 10000 AND f.code < 15000", Foo.class);
		
		foos = query.getResultList();
		
		watch = Stopwatch.createStarted();
		for (Foo foo : foos) {
			foo.setDescription("UPDATED");
			entityManager.merge(foo);
		}
		entityManager.flush();
		log("Updating entities iteratively with single flush.", watch.elapsed(TimeUnit.NANOSECONDS));
		
		query = entityManager.createQuery("DELETE FROM Foo");
		query.executeUpdate();
		entityManager.clear();		
		
		initilizeDb(20000);
		
		//Fetch or build a list of entities that will later be updated.
		query = entityManager.createQuery("SELECT f FROM Foo f WHERE f.code > 10000 AND f.code < 15000", Foo.class);
		foos = query.getResultList();
		//...
		
		//This is value would be different for different target databases.
		final int MAX_IN_CLAUSE_FOR_DB = 1000;  
		int updated = 0;
		
		//Assuming we have some sort of list of items (and if we couldn't do the update in a single query).
		watch = Stopwatch.createStarted();
		List<List<Foo>> chunks = Lists.partition(foos, MAX_IN_CLAUSE_FOR_DB);
		for (List<Foo> chunk : chunks) {
			query = entityManager.createQuery("UPDATE Foo f SET f.description = :description WHERE f.id IN :ids");
			query.setParameter("description", "UPDATED");
			query.setParameter("ids", chunk.stream().map(Foo::getId).collect(Collectors.toList()));
			updated += query.executeUpdate();
		}		
		entityManager.flush();
		log("Updating entities if we already have the entities loaded and no single JPQL query will work.", watch.elapsed(TimeUnit.NANOSECONDS));
		Assert.assertEquals(4999, updated);
		
		query = entityManager.createQuery("DELETE FROM Foo");
		query.executeUpdate();
		entityManager.clear();		
		
		initilizeDb(20000);		
		query = entityManager.createQuery("UPDATE Foo f SET f.description = :description WHERE f.code > 10000 AND f.code < 15000");
		query.setParameter("description", "UPDATED");
		watch = Stopwatch.createStarted();
		query.executeUpdate();
		log("Updating entities in JPQL.", watch.elapsed(TimeUnit.NANOSECONDS));
		
	}
	
	@Test
	public void staleEntryAfterUpdate() {
		initilizeDb(1);
				
		Foo foo = (Foo) entityManager.createQuery("FROM Foo f WHERE f.code = :code")
				.setParameter("code", 0)
				.getSingleResult();
			
		int updated = entityManager.createQuery("UPDATE Foo f SET f.description = :description WHERE f.code = :code")
				.setParameter("description", "UPDATED")
				.setParameter("code", 0)
				.executeUpdate();
		
		Assert.assertEquals(1, updated);
		Assert.assertNotEquals("UPDATED", foo.getDescription());
		
		entityManager.refresh(foo);		
		
		Assert.assertEquals("UPDATED", foo.getDescription());		
	}	
		
	private static void log(String test, long nanos) {
		logger.info(String.format("Test %s took %d milliseconds.", test, TimeUnit.NANOSECONDS.toMillis(nanos)));
	}
}
