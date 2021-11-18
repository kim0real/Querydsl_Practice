package study.querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.JPQLQueryFactory;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {
    
    @Autowired
    EntityManager em;

    JPAQueryFactory jpaQueryFactory;

    @BeforeEach
    public void before() {
        jpaQueryFactory = new JPAQueryFactory(em);

        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL() {
        // member1을 찾아라
        Member findByJPQL = em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findByJPQL.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl() {
        //QMember m = new QMember("m");

        Member findMember = jpaQueryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne(); // 단건 조회

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() {
        Member findMember = jpaQueryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchAndParam() {
        Member findMember = jpaQueryFactory
                .selectFrom(member)
                .where(
                        member.username.eq("member1"),
                        member.age.eq(10) // 쉼표로 가르면 And로 이어진다.
                )
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() {
        List<Member> fetch = jpaQueryFactory
                .selectFrom(member)
                .fetch();

        Member fetchOne = jpaQueryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        Member fetchFirst = jpaQueryFactory
                .selectFrom(member)
                // .limit(1).fetchOne()
                .fetchFirst();

        QueryResults<Member> results = jpaQueryFactory
                .selectFrom(member)
                .fetchResults();

        results.getTotal();
        List<Member> content = results.getResults();

        long total = jpaQueryFactory
                .selectFrom(member)
                .fetchCount();

    }

    /**
     * 회원 정렬순서
     * 1. 회원나이 내림차순
     * 2. 회원이름 오름차순
     * 단 2에서 회원이름이 없으면 마지막에 출력(nulls last)
     */
    @Test
    public void sort() {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);

        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();
    }

    @Test
    public void paging() {
        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();

        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void paging2() {
        QueryResults<Member> queryResults = jpaQueryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();

        assertThat(queryResults.getTotal()).isEqualTo(4);
        assertThat(queryResults.getLimit()).isEqualTo(2);
        assertThat(queryResults.getOffset()).isEqualTo(1);
        assertThat(queryResults.getResults().size()).isEqualTo(2);

    }

    @Test
    public void aggregation() {
        List<Tuple> result = jpaQueryFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.min(),
                        member.age.max(),
                        member.age.avg()
                )
                .from(member)
                .fetch();

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
    }

    /**
     * 팀의 이름과 각 팀의 연령을 구해라
     */
    @Test
    public void group() throws Exception {
        List<Tuple> result = jpaQueryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15); // (10+15) / 2

        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35);
    }

    /**
     * 조인
     * 팀 A에 소속된 모든 회원
     */
    @Test
    public void join() {
        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }

    /**
     * 세타 조인
     * 회원의 이름이 팀 이름과 같은 회원
     * 연관 관계가 없는 필드로 조인
     * 외부 조인 불가능(조인 on을 사용하면 가능)
     */
    @Test
    public void theta_join() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        List<Member> result = jpaQueryFactory
                .select(member)
                .from(member, team) // 조인할 테이블을 그대로 나열
                .where(member.username.eq(team.name))
                .fetch();

        assertThat(result.size()).isEqualTo(2);
    }

    /**
     * 조인 on절
     * 1. 조인 대상 필터링
     * 2. 연관관계 없는 엔티티 외부 조인
     *
     * 회원과 팀을 조인하면서 팀 이름이 team인 팀만 조인, 회원은 모두 조회
     * JPQL : select m, t from Member m left join m.team t on t.name = 'teamA'
     *
     * 외부 조인이 아닌 내부 조인인 경우 굳이 on절에서 조건을 걸 필요 없이 where절에서 걸면 된다.
     */
    @Test
    public void join_on_filtering() {
        List<Tuple> result = jpaQueryFactory // 타입이 여러 개(Member, Team)이므로 튜플 타입
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team).on(team.name.eq("teamA"))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * 연관관계없는 엔티티 외부 조인
     * 회원의 이름이 팀 이름과 같은 회원
     *
     * leftJoin()부분에 일반 조인과 다르게 엔티티가 하나만 들어간다.
     */
    @Test
    public void join_on_no_relation() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = jpaQueryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();

        assertThat(result.size()).isEqualTo(2);
    }

    @PersistenceUnit
    EntityManagerFactory emf;

    /**
     * 실무에서 정말 많이 쓰이는 페치조인
     */
    @Test
    public void fetchJoinUse() {
        em.flush();
        em.clear();

        Member findMember = jpaQueryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치조인 적용").isTrue();
    }

    /**
     * 서브쿼리
     * 나이가 가장 많은 회원조회
     */
    @Test
    public void subQuery() {
        // 서브쿼리 안에서 사용할 별칭을 따로 만든다.
        QMember memberSub = new QMember("memberSub");

        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(40);
    }

    /**
     * 서브쿼리
     * 나이가 평균 이상인 회원
     */
    @Test
    public void subQueryGoe() {
        // 서브쿼리 안에서 사용할 별칭을 따로 만든다.
        QMember memberSub = new QMember("memberSub");

        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(30, 40);
    }

    /**
     * 서브쿼리 in
     * 나이가 평균 이상인 회원
     */
    @Test
    public void subQueryIn() {
        // 서브쿼리 안에서 사용할 별칭을 따로 만든다.
        QMember memberSub = new QMember("memberSub");

        List<Member> result = jpaQueryFactory
                .selectFrom(member)
                .where(member.age.in(
                        JPAExpressions
                                .select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(30, 40);
    }

    @Test
    public void selectSubquery() {
        // 서브쿼리 안에서 사용할 별칭을 따로 만든다.
        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = jpaQueryFactory
                .select(member.username,
                        JPAExpressions
                                .select(member.age.avg())
                                .from(memberSub))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * Case절
     */
    @Test
    public void basicCase() {
        List<String> result = jpaQueryFactory
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    public void complexCase() {
        List<String> result = jpaQueryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20살")
                        .when(member.age.between(20, 31)).then("21~30살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    public void constant() {
        List<Tuple> result = jpaQueryFactory
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * username_age
     * concat은 문자만 연결 가능하므로 age는 .stringValue()를 이용한다.
     * stringValue()은 enum타입에도 많이 쓰인다.
     */
    @Test
    public void concat() {
        List<String> result = jpaQueryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.username.eq("member1"))
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    /**
     * 프로젝션 대상이 하나 : 타입이 명확
     */
    @Test
    public void simpleProjection() {
        List<String> result = jpaQueryFactory
                .select(member.username)
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    /**
     * 프로젝션 대상이 둘 이상 : 튜플 또는 DTO로 조회
     */
    @Test
    public void tupleProjection() {
        List<Tuple> result = jpaQueryFactory
                .select(member.username, member.age)
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("member.username = " + tuple.get(member.username));
            System.out.println("member.age = " + tuple.get(member.age));
        }

    }

    /**
     * 순수 JPA에서 DTO 조회
     * new 명령어를 사용해야하며 DTO의 package이름을 다 적어줘야해서 지저분함.
     * 생성자 방식만 지원함
     * 
     * 반면에 QueryDSL는 DTO 조회 시
     * 프로퍼티 접근
     * 필드 직접접근
     * 생성자 사용
     * 3가지 방법 지원
     */
    @Test
    public void findDtoByJQPL() {
        List<MemberDto> result = em.createQuery("select new study.querydsl.dto.MemberDto(m.username, m.age) from Member m", MemberDto.class)
                .getResultList();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * QueryDSL DTO 조회
     * 1.프로퍼티 접근
     */
    @Test
    public void findDtoByQuerydsl() {
        List<MemberDto> result = jpaQueryFactory
                .select(Projections.bean(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * QueryDSL DTO 조회
     * 2.필드접근
     */
    @Test
    public void findDtoByQuerydsl2() {
        List<MemberDto> result = jpaQueryFactory
                .select(Projections.fields(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * QueryDSL DTO 조회
     * 3.생성자 사용
     */
    @Test
    public void findDtoByQuerydsl3() {
        List<MemberDto> result = jpaQueryFactory
                .select(Projections.constructor(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * QueryDSL DTO 조회
     * 필드 접근 시 필드의 변수명이 다를 경우
     * 필드나 서브쿼리에 별칭을 이용한다.
     */
    @Test
    public void findUserDto() {
        QMember memberSub = new QMember("memberSub");

        List<MemberDto> result = jpaQueryFactory
                .select(Projections.constructor(MemberDto.class,
                        member.username.as("name"), // 필드에 별칭 이용
                        ExpressionUtils.as(JPAExpressions
                                        .select(memberSub.age.max())
                                        .from(memberSub), "age") // 서브쿼리에 별칭 이용
                        ))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }
}
