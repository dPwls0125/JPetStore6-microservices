<%--

       Copyright 2010-2016 the original author or authors.

       Licensed under the Apache License, Version 2.0 (the "License");
       you may not use this file except in compliance with the License.
       You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing, software
       distributed under the License is distributed on an "AS IS" BASIS,
       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
       See the License for the specific language governing permissions and
       limitations under the License.

--%>
<%@ include file="../common/IncludeTop.jsp"%>

<div id="Catalog"><form action="/account/editAccount" method="post">

	<h3>User Information</h3>

	<table>
		<tr>
			<td>User ID:</td>
			<td>${sessionScope.account.username}</td>
			<input type="hidden" name="username" value="${sessionScope.account.username}">
			<input type="hidden" name="csrf" value="${sessionScope.csrf_token}">
		</tr>
		<tr>
			<td>New password:</td>
			<td><input type="password" name="password"></td>
		</tr>
		<tr>
			<td>Repeat password:</td>
			<td><input type="password" name="repeatedPassword"></td>
		</tr>
	</table>
	<%@ include file="IncludeAccountFields.jsp"%>

	<input type="submit" value="Save Account Information">

</form><a href="/order/listOrders">My Orders</a></div>

<%@ include file="../common/IncludeBottom.jsp"%>
