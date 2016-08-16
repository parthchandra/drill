/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <string>
#include <vector>

#include <boost/assign.hpp>
#include <boost/shared_ptr.hpp>

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include "drill/collections.hpp"

namespace {
template<typename T, typename Iter>
class DrillVectorIteratorImpl: public Drill::impl::DrillIteratorImpl<T> {
public:
	typedef DrillVectorIteratorImpl<T, Iter> type;
	typedef Drill::impl::DrillIteratorImpl<T> supertype;

	DrillVectorIteratorImpl(const Iter& it): m_it(it) {};

	T& operator*() const { return m_it.operator *();}
	T* operator->() const { return m_it.operator->(); }

	operator typename Drill::impl::DrillIteratorImpl<const T>::iterator_ptr() const { return typename Drill::impl::DrillIteratorImpl<const T>::iterator_ptr(new DrillVectorIteratorImpl<const T, Iter>(m_it)); }

	DrillVectorIteratorImpl& operator++() {
		m_it++; return *this;
	}

	bool operator==(const supertype& x) const {
		const type& other(dynamic_cast<const type&>(x));
		return m_it == other.m_it;
	}

	bool operator!=(const supertype& x) const { return !(*this==x); }

private:
	Iter m_it;
};

template<typename T>
class DrillVectorImpl: public Drill::impl::DrillCollectionImpl<T> {
public:
	typedef Drill::impl::DrillCollectionImpl<T> supertype;

	typedef typename supertype::iterator_ptr iterator_ptr;
	typedef typename supertype::const_iterator_ptr const_iterator_ptr;

	DrillVectorImpl() {}
	DrillVectorImpl(const std::vector<T>& v): m_vector(v) {};

	iterator_ptr begin() { return iterator_ptr(new IteratorImpl(m_vector.begin()));}
	const_iterator_ptr begin() const { return const_iterator_ptr(new ConstIteratorImpl(m_vector.begin()));}
	iterator_ptr end() { return iterator_ptr(new IteratorImpl(m_vector.end()));}
	const_iterator_ptr end() const { return const_iterator_ptr(new ConstIteratorImpl(m_vector.end()));}

private:
	typedef DrillVectorIteratorImpl<T, typename std::vector<T>::iterator> IteratorImpl;
	typedef DrillVectorIteratorImpl<const T, typename std::vector<T>::const_iterator> ConstIteratorImpl;
	std::vector<T> m_vector;
};

template<typename T>
class DrillVector: public Drill::DrillCollection<T> {
public:
	DrillVector(const std::vector<T>& v): Drill::DrillCollection<T>(typename Drill::DrillCollection<T>::ImplPtr(new DrillVectorImpl<T>(v))) {}
};

} // anonymous namespace

class CollectionsTest: public CppUnit::TestFixture {
public:
	CollectionsTest() {}

	CPPUNIT_TEST_SUITE( CollectionsTest );
	CPPUNIT_TEST( testDrillVector );
	CPPUNIT_TEST( testConstDrillVector );
	CPPUNIT_TEST_SUITE_END();

	void testDrillVector() {
		std::vector<std::string> v = boost::assign::list_of("foo")("bar");

		DrillVector<std::string> drillCollection(v);
		std::vector<std::string> result;
		for(DrillVector<std::string>::const_iterator it = drillCollection.begin(); it != drillCollection.end(); ++it) {
			result.push_back(*it);
		}

		CPPUNIT_ASSERT(result == v);
	}

	void testConstDrillVector() {
		std::vector<std::string> v = boost::assign::list_of("foo")("bar");

		const DrillVector<std::string> drillCollection(v);
		std::vector<std::string> result;
		for(DrillVector<std::string>::const_iterator it = drillCollection.begin(); it != drillCollection.end(); ++it) {
			result.push_back(*it);
		}

		CPPUNIT_ASSERT(result == v);
	}
};

CPPUNIT_TEST_SUITE_REGISTRATION( CollectionsTest );
