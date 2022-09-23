package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    
    @Resource
    private IUserService userService;

    @Resource
    private IFollowService followService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if (blog == null){
            return Result.fail("笔记不存在！");
        }
        queryBlogByUser(blog);
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        UserDTO user = UserHolder.getUser();
        if (user == null){
            return;
        }
        Double score = stringRedisTemplate.opsForZSet().score(BLOG_LIKED_KEY + blog.getId(), user.getId().toString());
        blog.setIsLike(score != null);
    }

    @Override
    public Result queryHotBlog(Integer current) {
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            queryBlogByUser(blog);
            isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result likeBlog(Long id) {
        String userId = UserHolder.getUser().getId().toString();
        Double score = stringRedisTemplate.opsForZSet().score(BLOG_LIKED_KEY + id,  userId);
        if (score == null){
            if(update().setSql("liked = liked + 1").eq("id", id).update()){
                stringRedisTemplate.opsForZSet().add(BLOG_LIKED_KEY + id,userId,System.currentTimeMillis());
            }
        } else {
            if(update().setSql("liked = liked - 1").eq("id", id).update()){
                stringRedisTemplate.opsForZSet().remove(BLOG_LIKED_KEY + id,userId);
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(BLOG_LIKED_KEY + id, 0, 4);
        if (top5 == null || top5.isEmpty()){
            return Result.ok(Collectors.toList());
        }
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        List<UserDTO> userDTOS = userService.query()
                            .in("id",ids).last("ORDER BY FIELD(id,"+ StrUtil.join(",",ids) + ")").list()
                            .stream()
                            .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                            .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        Long userId = UserHolder.getUser().getId();
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet().reverseRangeByScoreWithScores(FEED_KEY + userId, 0, max, offset, 2);
        if (typedTuples == null || typedTuples.isEmpty()){
            return Result.ok();
        }
        long minTime = 0;
        int os = 1;
        List<Long> ids = new ArrayList<>();
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            ids.add(Long.valueOf(typedTuple.getValue()));
            long time = typedTuple.getScore().longValue();
            if (minTime == time){
                os ++;
            }else {
                minTime = time;
                os = 1;
            }
        }
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Blog blog : blogs) {
            queryBlogByUser(blog);
            isBlogLiked(blog);
        }
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setOffset(os);
        scrollResult.setMinTime(minTime);
        scrollResult.setList(blogs);
        return Result.ok(scrollResult);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        save(blog);
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        for (Follow follow : follows) {
            stringRedisTemplate.opsForZSet().add(FEED_KEY+follow.getUserId(),blog.getId().toString(),System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());
    }

    private void queryBlogByUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
