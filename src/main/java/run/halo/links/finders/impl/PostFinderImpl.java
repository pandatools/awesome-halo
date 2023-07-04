package run.halo.links.finders.impl;


import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.comparator.Comparators;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import run.halo.app.core.extension.Counter;
import run.halo.app.core.extension.content.Post;
import run.halo.app.core.extension.content.Snapshot;
import run.halo.app.extension.ListResult;
import run.halo.app.extension.MetadataUtil;
import run.halo.app.extension.ReactiveExtensionClient;

import run.halo.app.theme.finders.Finder;
import run.halo.app.metrics.MeterUtils;
import run.halo.links.ContentWrapper;
import run.halo.links.MyCategoryFinder;
import run.halo.links.MyPostFinder;
// import run.halo.links.MyPostPublicQueryService;
import run.halo.links.vo.CategoryTreeVo;
import run.halo.links.vo.ContentVo;
import run.halo.links.vo.MyListedPostVo;
import run.halo.links.vo.MyPostVo;
import run.halo.links.vo.MyStatsVo;

/**
 * A finder for {@link Post}.
 *
 * @author guqing
 * @since 2.0.0
 */
@Finder("mypostFinder")
@AllArgsConstructor
public class PostFinderImpl implements MyPostFinder {
    private final MyCategoryFinder categoryFinder;

    private final ReactiveExtensionClient client;


    @Override
    public Mono<MyPostVo> getByName(String postName) {
        System.out.println("eeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        return client.get(Post.class, postName)
            .map(MyPostVo::from)
            .flatMap(postVo -> {
                System.out.println(
                    "flatMapflatMapflatMapflatMap:postVo = " + postVo.getMetadata2());

                Mono<MyPostVo> myPostVoMono = content(postName)
                    .doOnNext(postVo::setContent)
                    .thenReturn(postVo);
                return myPostVoMono;
            });

    }

    @Override
    public Map<String,String> getAnnotationsByArticle(String name, String patternString) {
        MyPostVo obj = this.getByName(name).block();
        Map<String, String> result = new HashMap<>();
        Map<String, String> annotations = obj.getMetadata().getAnnotations();
        for (String key : annotations.keySet()) {
            Pattern pattern = Pattern.compile(patternString);
            Matcher matcher = pattern.matcher(key);
            if (matcher.find()) {
                result.put(key, annotations.get(key));
            }
        }
        return result;
    }
    private boolean contains(List<String> c, List<String> keys) {
        if(c == null){
            return false;
        }
        for(String key:keys) {
            if (StringUtils.isBlank(key)) {
                continue;
            }
            if (c.contains(key)){
                return true;
            }
        }
        return false;
    }

    private <T extends MyListedPostVo> Mono<MyStatsVo> populateStats(T postVo) {
        return
            client.fetch(Counter.class, MeterUtils.nameOf(Post.class, postVo.getMetadata().getName())).map(counter -> MyStatsVo.builder()
                .visit(counter.getVisit())
                .upvote(counter.getUpvote())
                .comment(counter.getApprovedComment())
                .build()
            ).defaultIfEmpty(MyStatsVo.empty());

    }

    int pageNullSafe(Integer page) {
        return ObjectUtils.defaultIfNull(page, 1);
    }

    int sizeNullSafe(Integer size) {
        return ObjectUtils.defaultIfNull(size, 10);
    }
    @Override
    public Mono<ListResult<MyListedPostVo>> listByCategoryAndChildren(@Nullable Integer page,
        @Nullable Integer size,
        String categoryName){
        System.out.println("listByCategoryAndChildren!!!!");
        CategoryTreeVo categoryTreeVo = categoryFinder.getTreeByNameChild(categoryName);

        List<String> result = new ArrayList<>();
        categoryFinder.traverse(categoryTreeVo,result);
        System.out.println("yslhhhresult = " + result);
        Comparator<Post> comparator =  defaultComparator();
        Predicate<Post> postPredicate = post -> contains(post.getSpec().getCategories(), result);
        Predicate<Post> FIXED_PREDICATE = post -> post.isPublished()
            && Objects.equals(false, post.getSpec().getDeleted())
            && Post.VisibleEnum.PUBLIC.equals(post.getSpec().getVisible());

        Predicate<Post> predicate = FIXED_PREDICATE
            .and(postPredicate == null ? post -> true : postPredicate);
        Mono<ListResult<MyListedPostVo>> listResultMono = client.list(Post.class, predicate,
                comparator, pageNullSafe(page), sizeNullSafe(size))
            .flatMap(list -> Flux.fromStream(list.get())
                .concatMap(post -> convertToListedPostVo(post)
                    .flatMap(postVo -> populateStats(postVo)
                        .doOnNext(postVo::setStats).thenReturn(postVo)
                    )
                )
                .collectList()
                .map(postVos -> new ListResult<>(list.getPage(), list.getSize(), list.getTotal(),
                    postVos)
                )
            )
            .defaultIfEmpty(new ListResult<>(page, size, 0L, List.of()));
        System.out.println("ok!!!!!!!!");
        return listResultMono;
    }


    protected void checkBaseSnapshot(Snapshot snapshot) {
        Assert.notNull(snapshot, "The snapshot must not be null.");
        String keepRawAnno =
            MetadataUtil.nullSafeAnnotations(snapshot).get(Snapshot.KEEP_RAW_ANNO);
        if (!org.thymeleaf.util.StringUtils.equals(Boolean.TRUE.toString(), keepRawAnno)) {
            throw new IllegalArgumentException(
                String.format("The snapshot [%s] is not a base snapshot.",
                    snapshot.getMetadata().getName()));
        }
    }

    public Mono<ContentWrapper> getContent(String snapshotName, String baseSnapshotName) {
        return client.fetch(Snapshot.class, baseSnapshotName)
            .doOnNext(this::checkBaseSnapshot)
            .flatMap(baseSnapshot -> {
                if (StringUtils.equals(snapshotName, baseSnapshotName)) {
                    var contentWrapper = ContentWrapper.patchSnapshot(baseSnapshot, baseSnapshot);
                    return Mono.just(contentWrapper);
                }
                return client.fetch(Snapshot.class, snapshotName)
                    .map(snapshot -> ContentWrapper.patchSnapshot(snapshot, baseSnapshot));
            });
    }


    // @Override
    public Mono<ContentWrapper> getReleaseContent(String postName) {
        return client.get(Post.class, postName)
            .flatMap(post -> {
                String releaseSnapshot = post.getSpec().getReleaseSnapshot();
                return this.getContent(releaseSnapshot, post.getSpec().getBaseSnapshot());
            });
    }

    // @Override
    public Mono<ContentVo> content(String postName) {
        return this.getReleaseContent(postName)
            .map(wrapper -> ContentVo.builder().content(wrapper.getContent())
                .raw(wrapper.getRaw()).build());
    }


    private Mono<MyPostVo> fetchByName(String name) {
        if (StringUtils.isBlank(name)) {
            return Mono.empty();
        }
        return getByName(name)
            .onErrorResume(MyExtensionNotFoundException.class::isInstance, (error) -> Mono.empty());
    }

    public Mono<MyListedPostVo> convertToListedPostVo(@NonNull Post post) {
        Assert.notNull(post, "Post must not be null");
        MyListedPostVo postVo = MyListedPostVo.from(post);
        postVo.setCategories(List.of());
        postVo.setTags(List.of());
        postVo.setContributors(List.of());

        return Mono.just(postVo)
            .defaultIfEmpty(postVo);
    }

    @Override
    public Flux<MyListedPostVo> listAll() {


        var list = client.list(Post.class, post -> post.isPublished()
                    && Objects.equals(false, post.getSpec().getDeleted())
                    && Post.VisibleEnum.PUBLIC.equals(post.getSpec().getVisible())

                , defaultComparator())
            .concatMap(this::convertToListedPostVo);
        return list;
    }


    static Comparator<Post> defaultComparator() {
        Function<Post, Boolean> pinned =
            post -> Objects.requireNonNullElse(post.getSpec().getPinned(), false);
        Function<Post, Integer> priority =
            post -> Objects.requireNonNullElse(post.getSpec().getPriority(), 0);
        Function<Post, Instant> publishTime =
            post -> post.getSpec().getPublishTime();
        Function<Post, String> name = post -> post.getMetadata().getName();
        return Comparator.comparing(pinned)
            .thenComparing(priority)
            .thenComparing(publishTime, Comparators.nullsLow())
            .thenComparing(name)
            .reversed();
    }
}
